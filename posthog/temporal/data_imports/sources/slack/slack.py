from collections.abc import Iterator
from typing import Any, Optional

import requests
from dlt.sources.helpers.requests import Request, Response
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import BasePaginator

from posthog.temporal.data_imports.pipelines.pipeline.typings import SourceResponse
from posthog.temporal.data_imports.sources.common.rest_source import RESTAPIConfig, rest_api_resources
from posthog.temporal.data_imports.sources.common.rest_source.typing import EndpointResource


class SlackCursorPaginator(BasePaginator):
    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        res = response.json()

        self._next_cursor = None

        if not res or not res.get("ok"):
            self._has_next_page = False
            return

        next_cursor = res.get("response_metadata", {}).get("next_cursor", "")

        if next_cursor:
            self._next_cursor = next_cursor
            self._has_next_page = True
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if self._next_cursor:
            request.params = {**(request.params or {}), "cursor": self._next_cursor}


def get_resource(name: str, should_use_incremental_field: bool) -> EndpointResource:
    if name == "channels":
        return {
            "name": "channels",
            "table_name": "channels",
            "primary_key": "id",
            "write_disposition": "replace",
            "endpoint": {
                "data_selector": "channels",
                "path": "conversations.list",
                "params": {
                    "types": "public_channel,private_channel",
                    "limit": 200,
                    "exclude_archived": "false",
                },
            },
            "table_format": "delta",
        }

    if name == "users":
        return {
            "name": "users",
            "table_name": "users",
            "primary_key": "id",
            "write_disposition": "replace",
            "endpoint": {
                "data_selector": "members",
                "path": "users.list",
                "params": {
                    "limit": 200,
                },
            },
            "table_format": "delta",
        }

    raise ValueError(f"Unknown Slack resource: {name}")


def _fetch_all_channels(access_token: str) -> list[dict[str, Any]]:
    channels: list[dict[str, Any]] = []
    cursor: str | None = None
    url = "https://slack.com/api/conversations.list"
    headers = {"Authorization": f"Bearer {access_token}"}

    while True:
        params: dict[str, Any] = {
            "types": "public_channel,private_channel",
            "limit": 200,
            "exclude_archived": "false",
        }
        if cursor:
            params["cursor"] = cursor

        response = requests.get(url, headers=headers, params=params, timeout=30)
        data = response.json()

        if not data.get("ok"):
            error = data.get("error", "unknown_error")
            raise Exception(f"Slack API error fetching channels: {error}")

        channels.extend(data.get("channels", []))

        next_cursor = data.get("response_metadata", {}).get("next_cursor", "")
        if next_cursor:
            cursor = next_cursor
        else:
            break

    return channels


def _fetch_messages_for_channel(
    access_token: str,
    channel_id: str,
    oldest: str | None = None,
) -> Iterator[dict[str, Any]]:
    cursor: str | None = None
    url = "https://slack.com/api/conversations.history"
    headers = {"Authorization": f"Bearer {access_token}"}

    while True:
        params: dict[str, Any] = {
            "channel": channel_id,
            "limit": 200,
        }
        if oldest:
            params["oldest"] = oldest
        if cursor:
            params["cursor"] = cursor

        response = requests.get(url, headers=headers, params=params, timeout=30)
        data = response.json()

        if not data.get("ok"):
            error = data.get("error", "unknown_error")
            # Skip channels we can't access rather than failing the whole sync
            if error in ("channel_not_found", "not_in_channel", "missing_scope"):
                return
            raise Exception(f"Slack API error fetching messages for channel {channel_id}: {error}")

        messages = data.get("messages", [])
        for msg in messages:
            msg["channel_id"] = channel_id
            yield msg

        next_cursor = data.get("response_metadata", {}).get("next_cursor", "")
        if next_cursor:
            cursor = next_cursor
        else:
            break


def _messages_generator(
    access_token: str,
    oldest: str | None = None,
) -> Iterator[dict[str, Any]]:
    channels = _fetch_all_channels(access_token)
    for channel in channels:
        yield from _fetch_messages_for_channel(access_token, channel["id"], oldest)


def slack_source(
    access_token: str,
    endpoint: str,
    team_id: int,
    job_id: str,
    should_use_incremental_field: bool = False,
    db_incremental_field_last_value: Optional[Any] = None,
    incremental_field: str | None = None,
) -> SourceResponse:
    if endpoint == "messages":
        oldest = (
            str(db_incremental_field_last_value)
            if should_use_incremental_field and db_incremental_field_last_value
            else None
        )

        return SourceResponse(
            name="messages",
            items=lambda: _messages_generator(access_token, oldest),
            primary_keys=["channel_id", "ts"],
            partition_count=1,
            partition_size=1,
        )

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://slack.com/api/",
            "auth": BearerTokenAuth(token=access_token),
            "paginator": SlackCursorPaginator(),
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "replace",
        },
        "resources": [get_resource(endpoint, should_use_incremental_field)],
    }

    resources = rest_api_resources(config, team_id, job_id, None)
    assert len(resources) == 1
    resource = resources[0]

    return SourceResponse(
        name=endpoint,
        items=lambda: resource,
        primary_keys=["id"],
        partition_count=1,
        partition_size=1,
    )


def validate_credentials(access_token: str) -> bool:
    url = "https://slack.com/api/auth.test"
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        return data.get("ok", False)
    except Exception:
        return False
