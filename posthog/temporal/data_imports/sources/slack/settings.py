from products.data_warehouse.backend.types import IncrementalField

ENDPOINTS = ("channels", "users", "messages")

INCREMENTAL_FIELDS: dict[str, list[IncrementalField]] = {
    "channels": [],
    "users": [],
    "messages": [],
}
