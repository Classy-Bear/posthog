from products.data_warehouse.backend.types import IncrementalField, IncrementalFieldType

ENDPOINTS = ("channels", "users", "messages")

INCREMENTAL_FIELDS: dict[str, list[IncrementalField]] = {
    "channels": [],
    "users": [],
    "messages": [
        {
            "label": "ts",
            "type": IncrementalFieldType.Timestamp,
            "field": "ts",
            "field_type": IncrementalFieldType.Timestamp,
        },
    ],
}
