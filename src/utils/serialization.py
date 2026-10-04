from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from bson import Decimal128, ObjectId


def serialize_mongo_document(value: Any) -> Any:
    """Recursively converts MongoDB/BSON types into standard JSON-compatible Python types.

    Preserves data integrity:
    - ObjectId -> str
    - datetime / date -> ISO 8601 string
    - Decimal128 / Decimal -> float (or int if whole)
    - dict -> recursively processed dict
    - list / tuple / set -> recursively processed list
    - bytes -> hex or utf-8 decoded string
    """
    if value is None:
        return None

    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal128):
        decimal_val = value.to_decimal()
        return int(decimal_val) if decimal_val % 1 == 0 else float(decimal_val)

    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)

    if isinstance(value, dict):
        return {str(k): serialize_mongo_document(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [serialize_mongo_document(item) for item in value]

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()

    return value
