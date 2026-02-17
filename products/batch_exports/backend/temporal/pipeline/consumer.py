import abc
import enum
import time
import typing
import asyncio
import operator
import dataclasses
import collections.abc

import pyarrow as pa
import temporalio.common

from posthog.temporal.common.logger import get_logger, get_write_only_logger

from products.batch_exports.backend.temporal.metrics import get_bytes_exported_metric, get_rows_exported_metric
from products.batch_exports.backend.temporal.pipeline.producer import Producer
from products.batch_exports.backend.temporal.pipeline.transformer import ChunkTransformerProtocol
from products.batch_exports.backend.temporal.pipeline.types import BatchExportError, BatchExportResult
from products.batch_exports.backend.temporal.spmc import RecordBatchQueue, raise_on_task_failure
from products.batch_exports.backend.temporal.utils import cast_record_batch_json_columns

LOGGER = get_write_only_logger(__name__)
EXTERNAL_LOGGER = get_logger("EXTERNAL")


class _WaitResult(enum.Enum):
    """Enumeration of possible results when concurrently waiting for two tasks."""

    FIRST_DONE = (True, False)
    SECOND_DONE = (False, True)
    BOTH_DONE = (True, True)


class Consumer:
    """Consumer for batch exports.

    This is an alternative implementation of the `spmc.Consumer` class that consumes data from a producer which is in
    turn reading data from the internal S3 staging area.
    """

    def __init__(self, model: str = "events"):
        self.logger = LOGGER.bind()
        self.external_logger = EXTERNAL_LOGGER.bind()
        self.model = model

        # Progress tracking
        self.total_record_batches_count = 0
        self.total_records_count = 0
        self.total_record_batch_bytes_count = 0
        self.total_file_bytes_count = 0
        self.done = False

    @property
    def rows_exported_counter(self) -> temporalio.common.MetricCounter:
        """Access the rows exported metric counter."""
        return get_rows_exported_metric(model=self.model)

    @property
    def bytes_exported_counter(self) -> temporalio.common.MetricCounter:
        """Access the bytes exported metric counter."""
        return get_bytes_exported_metric(model=self.model)

    def reset_tracking(self) -> None:
        self.total_record_batches_count = 0
        self.total_records_count = 0
        self.total_record_batch_bytes_count = 0
        self.total_file_bytes_count = 0
        self.done = False

    async def start(
        self,
        queue: RecordBatchQueue,
        producer_task: asyncio.Task,
        transformer: ChunkTransformerProtocol,
        json_columns: collections.abc.Iterable[str] = ("properties", "person_properties", "set", "set_once"),
    ) -> BatchExportResult:
        """Start consuming record batches from queue.

        Record batches will be processed by the `transformer`, which transforms the
        record batch into chunks of bytes, depending on the `file_format` and
        `compression`.
        Each of these chunks will be consumed by the `consume_chunk` method, which is
        implemented by subclasses.
        Returns:
            BatchExportResult:
                - The total number of records in all consumed record batches. If an
                  error occurs, this will be None.
                - The total number of bytes exported (this is the size of the actual data
                  exported, which takes into account the file type and compression). If
                  an error occurs, this will be None.
                - The error that occurred, if any. If no error occurred, this will be
                  None. If an error occurs, this will be a string representation of the
                  error.
        """

        self.reset_tracking()

        self.logger.info("Starting consumer from internal S3 stage")

        try:
            async for chunk, is_eof in transformer.iter(
                self.generate_record_batches_from_queue(queue, producer_task, json_columns),
            ):
                chunk_size = len(chunk)
                self.total_file_bytes_count += chunk_size

                await self.consume_chunk(data=chunk)
                self.bytes_exported_counter.add(chunk_size)

                if is_eof:
                    await self.finalize_file()

            await self.finalize()

        except Exception:
            self.logger.exception("Unexpected error occurred while consuming record batches")
            raise
        finally:
            self.done = True

        self.logger.info(
            f"Finished consuming {self.total_records_count:,} records, {self.total_record_batch_bytes_count / 1024**2:.2f} MiB "
            f"from {self.total_record_batches_count:,} record batches. "
            f"Total file MiB: {self.total_file_bytes_count / 1024**2:.2f}"
        )

        return BatchExportResult(self.total_records_count, self.total_file_bytes_count)

    async def generate_record_batches_from_queue(
        self,
        queue: RecordBatchQueue,
        producer_task: asyncio.Task,
        json_columns: collections.abc.Iterable[str] = ("properties", "person_properties", "set", "set_once"),
    ):
        """Yield record batches from provided `queue` until `producer_task` is done.

        This method is non-blocking by concurrently waiting for both `queue.get` and
        `producer_task` in a loop using `asyncio.wait`.

        Everytime `queue.get` returns a value it is yielded. Whenever `producer_task` is
        done, we check if `queue` is empty. If it is, then the method doesn't expect
        anything else to ever come in the queue, and thus can exit without data loss
        (after canceling a pending `queue.get` to avoid resource leaking).

        If the `queue` is not empty, then the method can't exit just yet and instead
        continues waiting on `queue.get`.
        """

        while True:
            get_task = asyncio.create_task(queue.get())
            _ = await asyncio.wait((get_task, producer_task), return_when=asyncio.FIRST_COMPLETED)

            wait_result = _WaitResult((get_task.done(), producer_task.done()))
            match wait_result:
                case _WaitResult.FIRST_DONE | _WaitResult.BOTH_DONE:
                    record_batch = get_task.result()

                case _WaitResult.SECOND_DONE:
                    if queue.empty():
                        self.logger.debug(
                            "Empty queue with no more events being produced, closing writer loop and flushing"
                        )
                        get_task.cancel()
                        break
                    else:
                        record_batch = await get_task

                case _:
                    typing.assert_never(wait_result)

            self.logger.info(f"Consuming batch number {self.total_record_batches_count}")

            if json_columns:
                record_batch = cast_record_batch_json_columns(record_batch, json_columns=json_columns)

            yield record_batch

            self.track_record_batch(record_batch)

    def track_record_batch(self, record_batch: pa.RecordBatch) -> None:
        """Track consumer progress based on the last consumed record batch."""

        num_records_in_batch = record_batch.num_rows
        num_bytes_in_batch = record_batch.nbytes

        self.total_records_count += num_records_in_batch
        self.total_record_batch_bytes_count += num_bytes_in_batch
        self.rows_exported_counter.add(num_records_in_batch)

        self.logger.debug(
            f"Consumed batch number {self.total_record_batches_count} with "
            f"{num_records_in_batch:,} records, {num_bytes_in_batch / 1024**2:.2f} "
            f"MiB. Total records consumed so far: {self.total_records_count:,}, "
            f"total MiB consumed so far: {self.total_record_batch_bytes_count / 1024**2:.2f}, "
            f"total file MiB consumed so far: {self.total_file_bytes_count / 1024**2:.2f}"
        )

        self.total_record_batches_count += 1

    @abc.abstractmethod
    async def consume_chunk(self, data: bytes):
        """Consume a chunk of data."""
        pass

    @abc.abstractmethod
    async def finalize_file(self):
        """Finalize the current file.

        Only called if working with multiple files, such as when we have a max file size.
        """
        pass

    @abc.abstractmethod
    async def finalize(self):
        """Finalize the consumer."""
        pass


async def run_consumer_from_stage(
    queue: RecordBatchQueue,
    consumer: Consumer,
    producer_task: asyncio.Task[None],
    transformer: ChunkTransformerProtocol,
    json_columns: collections.abc.Iterable[str] = ("properties", "person_properties", "set", "set_once"),
) -> BatchExportResult:
    """Run a record batch consumer to batch export to a destination.

    The consumer reads record from a queue populated by a producer that fetches them
    from an the internal S3 bucket.

    Arguments:
        queue: The queue to consume record batches from.
        consumer: The consumer to run.
        producer_task: The task that produces record batches.
        transformer: The transformer used to convert record batches into their desired
            export format.

    Returns:
        BatchExportResult (A tuple containing):
            - The total number of records in all consumed record batches.
            - The total number of bytes exported (this is the size of the actual data
                exported, which takes into account the file type and compression).
    """
    result = await consumer.start(
        queue=queue,
        producer_task=producer_task,
        transformer=transformer,
        json_columns=json_columns,
    )

    await raise_on_task_failure(producer_task)
    return result


_GET_TOTAL_RECORD_BATCH_BYTES_COUNT = operator.attrgetter("total_record_batch_bytes_count")


class Tracker:
    """Tracks progress of one or more consumers.

    This progress tracking can be used to decide when to scale up the number of
    consumers.
    """

    def __init__(self, poll_delay: int | float, window_size: int):
        self.consumers: set[Consumer] = set()
        self.poll_delay = poll_delay
        self.window_size = window_size

        # Progress tracking
        self.bytes_consumed = 0
        self.bytes_consumed_window = 0
        self.time_elapsed: int | float = 0
        self.time_elapsed_window: int | float = 0
        self._start_time: int | float | None = None
        self._window_counter = 0
        self.__window_start_time: int | float | None = None

    @property
    def start_time(self) -> float:
        """Tracking start time set on entering context."""
        if self._start_time is None:
            raise ValueError("tracker not started")
        return self._start_time

    @property
    def _window_start_time(self) -> float:
        """Tracking start time for every window."""
        if self.__window_start_time is None:
            raise ValueError("tracker not started")
        return self.__window_start_time

    @property
    def number_of_consumers(self) -> int:
        return len(self.consumers)

    @property
    def bytes_consumed_per_second(self) -> float:
        try:
            return self.bytes_consumed / self.time_elapsed
        except ZeroDivisionError:
            raise ValueError("tracker not started")

    @property
    def bytes_consumed_per_second_window(self) -> float:
        try:
            return self.bytes_consumed_window / self.time_elapsed_window
        except ZeroDivisionError:
            raise ValueError("tracker not started")

    async def __aenter__(self) -> typing.Self:
        self._start_time = self.__window_start_time = time.monotonic()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        self._start_time = None
        return None

    def track(self, consumer: Consumer) -> None:
        self.consumers.add(consumer)

    def poll(self) -> None:
        if self._window_counter == self.window_size:
            self._window_counter = 0
            self.bytes_consumed_window = 0
            self.time_elapsed_window = 0

        now = time.monotonic()
        current_total_bytes_consumed = sum(map(_GET_TOTAL_RECORD_BATCH_BYTES_COUNT, self.consumers))
        bytes_consumed_since_last_poll = current_total_bytes_consumed - self.bytes_consumed

        self.time_elapsed = now - self.start_time
        self.bytes_consumed = current_total_bytes_consumed
        self.bytes_consumed_window += bytes_consumed_since_last_poll
        self.time_elapsed_window = now - self._window_start_time

        if self._window_counter == self.window_size - 1:
            # NOTE: Counter increments by 1 right after so the next poll falls in a new window.
            # The time elapsed for that new window starts in the previous poll, i.e. now.
            self.__window_start_time = now

        self._window_counter += 1


DEFAULT_MAX_CONSUMERS = 5
DEFAULT_POLL_DELAY_SECONDS = 3
DEFAULT_TRACKING_WINDOW_SIZE = 5


@dataclasses.dataclass
class GroupSettings:
    """Settings for a consumer ``Group``.

    Attributes:
        target_duration_seconds: The duration the ``Group`` should aim for. It is
            recommended to set this to a fraction of the batch export interval, as we
            should always aim to finish with a good margin before the interval is up.
        poll_delay_seconds: How frequently the ``Group`` polls for metrics from all
            consumers. This represents a trade-off between time-to-scale and group
            overhead.
        tracking_window_size: ``Group`` only uses the last n observations to determine
            whether to add more consumers or not.
        max_consumers: Maximum number of consumers that can be added to a ``Group``.
    """

    target_duration_seconds: int
    poll_delay_seconds: int | float = DEFAULT_POLL_DELAY_SECONDS
    tracking_window_size: int = DEFAULT_TRACKING_WINDOW_SIZE
    max_consumers: int = DEFAULT_MAX_CONSUMERS


_C = typing.TypeVar("_C", bound=Consumer)


class Group(typing.Protocol[_C]):
    """A protocol to manage a group of consumers.

    The provided methods can run and scale up the group after the necessary required
    members are implemented:
    * ``build_consumer``: Should provide a fresh instance of a ``Consumer`` (or a
      subclass) to run.
    * ``start_consumer``: Provided with the instance, it should return the consumer
      task initialized by ``Consumer.start``.

    And ``GroupSettings`` are provided.

    Using this protocol allows whoever implements it to define how their consumers are
    initialized and started. As each destination has different requirements, a single
    class would not be able to represent all of them, so the flexibility of a protocol
    was needed.

    The group can only add consumers, not remove them. In the future this is a feature
    we may implement, but our main priority is that consumers can scale up to improve
    performance.
    """

    producer: Producer
    settings: GroupSettings
    records_completed: int = 0
    bytes_exported: int = 0
    _errors: list[BatchExportError] | None = None
    _tasks: set[asyncio.Task[BatchExportResult]] | None = None
    _tracker: Tracker | None = None

    def build_consumer(self) -> _C: ...

    def start_consumer(self, consumer: _C) -> collections.abc.Coroutine[None, None, BatchExportResult]: ...

    @property
    def result(self) -> BatchExportResult:
        """Accumulated result of all consumers in this group."""
        return BatchExportResult(
            records_completed=self.records_completed, bytes_exported=self.bytes_exported, error=self.errors or None
        )

    @property
    def done(self) -> bool:
        """If there is at least one task and all tasks are done, we are done."""
        return len(self.tasks) >= 1 and all(task.done() for task in self.tasks)

    @property
    def tracker(self) -> Tracker:
        """A ``Tracker`` for all consumers in this group."""
        if self._tracker is None:
            self._tracker = Tracker(self.settings.poll_delay_seconds, self.settings.tracking_window_size)
        return self._tracker

    @property
    def tasks(self) -> set[asyncio.Task[BatchExportResult]]:
        """A set for all consumer tasks in this group."""
        if self._tasks is None:
            self._tasks = set()
        return self._tasks

    @property
    def errors(self) -> list[BatchExportError]:
        """A list of all errors seen in consumers in this group."""
        if self._errors is None:
            self._errors = []
        return self._errors

    def _add_max_consumers(self, task_group: asyncio.TaskGroup) -> None:
        """Add as many new consumers to the group as possible."""
        while not self.tracker.number_of_consumers == self.settings.max_consumers:
            self._add_new_consumer(task_group)

    def _add_new_consumer(self, task_group: asyncio.TaskGroup) -> None:
        """Build a new consumer, start it, and add it to the group.

        This means the group starts tracking its progress.
        """
        consumer = self.build_consumer()
        task = task_group.create_task(self.start_consumer(consumer))
        task.add_done_callback(self._accumulate_results)
        self.tasks.add(task)
        self.tracker.track(consumer)

    def _accumulate_results(self, task: asyncio.Task[BatchExportResult]) -> None:
        """Accumulate results from successfully completed tasks."""
        assert task.done()

        if task.cancelled() or task.exception() is not None:
            # No results to accumulate
            return

        result = task.result()

        if result.records_completed is not None:
            self.records_completed += result.records_completed

        if result.bytes_exported is not None:
            self.bytes_exported += result.bytes_exported

        if result.error is not None:
            # TODO: Consolidate errors of the same type into one
            if not isinstance(result.error, list):
                errors = [result.error]
            else:
                errors = result.error

            self.errors.extend(errors)

    async def run(self) -> BatchExportResult:
        """Run the consumer group until batch export is done.

        Each consumer is handed over to an ``asyncio.TaskGroup`` to manage clean-up.

        By frequently polling from consumption rates from all consumers, this can
        estimate whether more consumers are needed to finish before
        ``self.settings.target_duration_seconds``.
        """
        async with (
            self.tracker as tracker,
            asyncio.TaskGroup() as tg,
        ):
            # At least one consumer will always be needed
            self._add_new_consumer(tg)

            while True:
                await asyncio.sleep(self.settings.poll_delay_seconds)

                if tracker.number_of_consumers == self.settings.max_consumers or self.done:
                    break

                tracker.poll()

                if tracker.time_elapsed > self.settings.target_duration_seconds:
                    # We overshot our target, scale as much as we can and exit
                    self._add_max_consumers(tg)
                    break

                else:
                    if not tracker.bytes_consumed or not tracker.bytes_consumed_per_second_window:
                        continue

                    bytes_left = self.producer.total_size - tracker.bytes_consumed
                    time_left = bytes_left / tracker.bytes_consumed_per_second_window

                    consumers_to_add = _compute_consumers_to_add(
                        bytes_left=bytes_left,
                        time_left=time_left,
                        bytes_consumption_rate=tracker.bytes_consumed_per_second_window,
                        current_consumers=tracker.number_of_consumers,
                        max_consumers=self.settings.max_consumers,
                    )
                    for _ in range(consumers_to_add):
                        self._add_new_consumer(tg)

        await raise_on_task_failure(self.producer.task)
        return self.result


def _compute_consumers_to_add(
    bytes_left: int,
    time_left: int | float,
    bytes_consumption_rate: int | float,
    current_consumers: int,
    max_consumers: int,
) -> int:
    """Compute the number of consumers to add to a group.

    This assumes the provided ``byte_consumption_rate`` will continue and uses it to
    compute how many consumers we would need to complete ``bytes_left`` in
    ``time_left``.

    The result will be bounded such that we will never add consumers to
    ``current_consumers`` above the ``max_consumers``, and the minimum number returned
    is 0, in which case no new consumers should be added.
    """
    target_bytes_consumption_rate = bytes_left / time_left
    bytes_consumption_rate_per_consumer = bytes_consumption_rate / current_consumers
    number_of_consumers_needed = target_bytes_consumption_rate / bytes_consumption_rate_per_consumer

    new_consumers_to_add = int(number_of_consumers_needed - current_consumers)
    # We could have negative new_consumers_to_add, so we bound it with a max(..., 0) as
    # we don't support subtracting consumers at the moment.
    bounded_consumers_to_add = min(max(new_consumers_to_add, 0), max_consumers - current_consumers)

    return bounded_consumers_to_add
