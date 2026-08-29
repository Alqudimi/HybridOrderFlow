from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
    "01234567890123456789",
)
PRICE_WORDS = {
    "ألف": 1000,
    "الف": 1000,
    "ألفان": 2000,
    "الفان": 2000,
    "ألفين": 2000,
    "ألفان فقط": 2000,
    "خمسة آلاف": 5000,
    "خمسه الاف": 5000,
    "خمسة الاف": 5000,
    "عشرة آلاف": 10000,
    "عشره الاف": 10000,
}
CURRENCY_ALIASES = {
    "ريال": "YER",
    "ريال يمني": "YER",
    "لاير": "YER",
    "لاير يمني": "YER",
    "yer": "YER",
}
STATUS_ALIASES = {
    "مؤكد": "مؤكد",
    "مؤكدة": "مؤكد",
    "تم التأكيد": "مؤكد",
    "مدفوع": "مدفوع",
    "تم الدفع": "تم الدفع",
    "قيد الانتظار": "قيد الانتظار",
    "بانتظار الدفع": "بانتظار الدفع",
    "قيد الشحن": "قيد الشحن",
    "مرتجع": "مرتجع",
}


def normalize_digits(value: str) -> str:
    return value.translate(ARABIC_DIGITS)


def _record_correction(
    corrections: list[dict[str, Any]],
    field: str,
    original: Any,
    corrected: Any,
    rule_code: str,
) -> None:
    if original != corrected:
        corrections.append(
            {
                "field": field,
                "original_value": original,
                "corrected_value": corrected,
                "rule_code": rule_code,
            }
        )


def _clean_text(
    record: dict[str, Any],
    field: str,
    corrections: list[dict[str, Any]],
) -> None:
    original = record.get(field)
    if original is None:
        return
    cleaned = str(original).strip()
    _record_correction(
        corrections, field, original, cleaned, "TRIM_WHITESPACE"
    )
    record[field] = cleaned


def _parse_number(
    value: Any,
    field: str,
    corrections: list[dict[str, Any]],
) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    original = value
    text = normalize_digits(str(value).strip())
    word_value = PRICE_WORDS.get(text)
    if word_value is not None:
        parsed = float(word_value)
        _record_correction(
            corrections, field, original, parsed, "PRICE_KNOWN_WORD"
        )
        return parsed
    text = re.sub(
        r"(ريال يمني|ريال|لاير يمني|لاير|YER)$", "", text
    ).strip()
    text = (
        text.replace("٫", ".")
        .replace("٬", ",")
        .replace("،", ",")
        .replace(" ", "")
        .replace(",", "")
    )
    try:
        parsed_decimal = Decimal(text)
    except (InvalidOperation, ValueError):
        return None
    parsed = float(parsed_decimal)
    if isinstance(value, (int, float)) and float(value) == parsed:
        return parsed
    _record_correction(
        corrections, field, original, parsed, "NUMBER_NORMALIZATION"
    )
    return parsed


def _normalize_currency(
    record: dict[str, Any], corrections: list[dict[str, Any]]
) -> None:
    original = record.get("currency")
    if original is None:
        return
    cleaned = str(original).strip()
    normalized = CURRENCY_ALIASES.get(cleaned.lower(), cleaned.upper())
    _record_correction(
        corrections, "currency", original, normalized, "CURRENCY_TO_YER"
    )
    record["currency"] = normalized


def _normalize_phone(
    record: dict[str, Any], corrections: list[dict[str, Any]]
) -> None:
    original = record.get("customer_phone")
    if original is None:
        return
    text = normalize_digits(str(original)).strip()
    compact = re.sub(r"[\s().-]", "", text)
    if compact.startswith("+967"):
        compact = compact[4:]
    elif compact.startswith("00967"):
        compact = compact[5:]
    if compact.isdigit() and len(compact) == 9:
        _record_correction(
            corrections,
            "customer_phone",
            original,
            compact,
            "PHONE_REMOVE_SPACES_AND_COUNTRY_CODE",
        )
        record["customer_phone"] = compact


def _normalize_email(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    errors: list[dict[str, str]],
) -> None:
    original = record.get("customer_email")
    if original is None:
        return
    text = str(original).strip()
    repaired = text.replace("@@", "@").replace("..", ".")
    if repaired != text and re.fullmatch(r"[^@\s]+@[^@\s.]+(?:\.[^@\s.]+)+", repaired):
        _record_correction(
            corrections,
            "customer_email",
            original,
            repaired,
            "EMAIL_REPEATED_SYMBOLS",
        )
        record["customer_email"] = repaired
    elif not re.fullmatch(r"[^@\s]+@[^@\s.]+(?:\.[^@\s.]+)+", repaired):
        errors.append(
            {
                "code": "INVALID_EMAIL",
                "detail": "Email is invalid and cannot be safely repaired",
            }
        )


def _normalize_date(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    errors: list[dict[str, str]],
) -> None:
    original = record.get("order_date")
    if original is None or str(original).strip() == "":
        errors.append(
            {
                "code": "INVALID_IMPOSSIBLE_DATE",
                "detail": "Order date is missing",
            }
        )
        return
    text = normalize_digits(str(original).strip())
    parsed: datetime | date | None = None
    for parser in (
        lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")),
        lambda value: datetime.strptime(value, "%d/%m/%Y"),
        lambda value: datetime.strptime(value, "%Y/%m/%d"),
        lambda value: datetime.strptime(value, "%Y-%m-%d"),
    ):
        try:
            parsed = parser(text)
            break
        except ValueError:
            continue
    if parsed is None:
        errors.append(
            {
                "code": "INVALID_IMPOSSIBLE_DATE",
                "detail": f"Date cannot be parsed safely: {original}",
            }
        )
        return
    normalized = (
        parsed.isoformat()
        if isinstance(parsed, datetime) and "T" in text
        else parsed.date().isoformat()
        if isinstance(parsed, datetime)
        else parsed.isoformat()
    )
    _record_correction(
        corrections, "order_date", original, normalized, "DATE_STANDARDIZE"
    )
    record["order_date"] = normalized


def _normalize_items(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    errors: list[dict[str, str]],
) -> float | None:
    original = record.get("items_json")
    if original is None or str(original).strip() == "":
        errors.append(
            {"code": "CORRUPTED_ITEMS_JSON", "detail": "Items JSON is missing"}
        )
        return None
    try:
        items = json.loads(original) if isinstance(original, str) else original
    except (TypeError, json.JSONDecodeError) as error:
        errors.append(
            {
                "code": "CORRUPTED_ITEMS_JSON",
                "detail": f"Items JSON is invalid: {error}",
            }
        )
        return None
    if not isinstance(items, list):
        errors.append(
            {"code": "CORRUPTED_ITEMS_JSON", "detail": "Items JSON is not a list"}
        )
        return None
    if not items:
        errors.append({"code": "EMPTY_ITEMS", "detail": "Order has no items"})
        return None
    normalized_items: list[dict[str, Any]] = []
    item_total = 0.0
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            errors.append(
                {
                    "code": "CORRUPTED_ITEMS_JSON",
                    "detail": f"Item {index} is not an object",
                }
            )
            continue
        qty = _parse_number(item.get("qty"), f"items[{index}].qty", corrections)
        unit_price = _parse_number(
            item.get("unit_price"), f"items[{index}].unit_price", corrections
        )
        if qty is None or unit_price is None:
            errors.append(
                {
                    "code": "UNKNOWN_PRICE",
                    "detail": f"Item {index} has missing/non-numeric price or quantity",
                }
            )
            continue
        if qty < 0 or unit_price < 0:
            errors.append(
                {
                    "code": "AMBIGUOUS_NEGATIVE_VALUE",
                    "detail": f"Item {index} has a negative quantity or price",
                }
            )
            continue
        normalized_item = dict(item)
        normalized_item["qty"] = qty
        normalized_item["unit_price"] = unit_price
        normalized_item["total"] = round(qty * unit_price, 2)
        item_total += normalized_item["total"]
        normalized_items.append(normalized_item)
    if len(normalized_items) != len(items):
        return None
    if normalized_items != items:
        _record_correction(
            corrections,
            "items_json",
            original,
            normalized_items,
            "ITEMS_NUMERIC_NORMALIZATION",
        )
    record["items_json"] = normalized_items
    return round(item_total, 2)


def classify_record(
    raw_record: dict[str, Any], duplicate_order_id: bool = False
) -> dict[str, Any]:
    """Clean one raw CSV record and return a final or quarantine document."""
    record = dict(raw_record)
    corrections: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for field in (
        "order_id",
        "customer_id",
        "customer_name",
        "city",
        "district",
        "delivery_type",
        "payment_method",
        "payment_status",
    ):
        _clean_text(record, field, corrections)
    if not record.get("order_id"):
        errors.append(
            {
                "code": "MISSING_ORDER_ID",
                "detail": "Order id is missing and cannot be inferred",
            }
        )
    if not record.get("customer_id"):
        errors.append(
            {
                "code": "MISSING_CUSTOMER_ID",
                "detail": "Customer id is missing and cannot be inferred",
            }
        )
    if duplicate_order_id and record.get("order_id"):
        errors.append(
            {
                "code": "DUPLICATE_ORDER_ID",
                "detail": "Duplicate business key within this input run",
            }
        )

    _normalize_currency(record, corrections)
    _normalize_phone(record, corrections)
    _normalize_email(record, corrections, errors)
    _normalize_date(record, corrections, errors)

    for field in ("delivery_cost", "payment_amount", "total_amount"):
        parsed = _parse_number(record.get(field), field, corrections)
        if parsed is not None:
            record[field] = parsed
            if parsed < 0:
                errors.append(
                    {
                        "code": "AMBIGUOUS_NEGATIVE_VALUE",
                        "detail": f"{field} is negative and its meaning is ambiguous",
                    }
                )

    for field in ("status", "payment_status", "delivery_type"):
        value = record.get(field)
        if value is not None:
            normalized = STATUS_ALIASES.get(str(value).strip(), str(value).strip())
            _record_correction(
                corrections, field, value, normalized, "DICTIONARY_STANDARDIZATION"
            )
            record[field] = normalized

    item_total = _normalize_items(record, corrections, errors)
    if item_total is not None and not errors:
        delivery_cost = float(record.get("delivery_cost") or 0)
        calculated_total = round(item_total + delivery_cost, 2)
        if record.get("total_amount") is None:
            errors.append(
                {
                    "code": "UNKNOWN_PRICE",
                    "detail": "Total amount is missing and cannot be safely inferred",
                }
            )
        elif abs(float(record["total_amount"]) - calculated_total) > 0.01:
            original_total = record["total_amount"]
            record["total_amount"] = calculated_total
            _record_correction(
                corrections,
                "total_amount",
                original_total,
                calculated_total,
                "TOTAL_RECALCULATE_FROM_ITEMS_AND_DELIVERY",
            )

    if errors:
        codes = [error["code"] for error in errors]
        if len(codes) > 1 and "MULTIPLE_CONFLICTING_ERRORS" not in codes:
            codes.append("MULTIPLE_CONFLICTING_ERRORS")
            errors.append(
                {
                    "code": "MULTIPLE_CONFLICTING_ERRORS",
                    "detail": "Multiple material errors prevent safe correction",
                }
            )
        return {
            **record,
            "quality_status": "quarantined",
            "error_codes": codes,
            "error_details": errors,
            "corrections": corrections,
            "raw_record": raw_record,
        }

    status = "corrected" if corrections else "valid"
    return {
        **record,
        "quality_status": status,
        "corrections": corrections,
        "raw_record": raw_record,
    }
