from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶٧٨٩",
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
    "yemeni riyal": "YER",
}

CITY_STANDARDIZATION = {
    "صنعا": "صنعاء",
    "صنعاء القديمة": "صنعاء",
    "الحديده": "الحديدة",
    "الحديده القديمة": "الحديدة",
    "عدن": "عدن",
    "تعز": "تعز",
    "أب": "إب",
    "اب": "إب",
    "مارب": "مأرب",
    "المكلا": "المكلا",
    "سيئون": "سيئون",
    "ذمار": "ذمار",
}

STATUS_ALIASES = {
    # --- Order Status ---
    "مؤكد": "مؤكد",
    "مؤكدة": "مؤكد",
    "تم التأكيد": "مؤكد",
    "confirmed": "مؤكد",
    "confirm": "مؤكد",
    "جديد": "جديد",
    "طلب جديد": "جديد",
    "new": "جديد",
    "قيد التنفيذ": "قيد التنفيذ",
    "تم الشحن": "قيد الشحن",
    "قيد الشحن": "قيد الشحن",
    "shipped": "قيد الشحن",
    "shipping": "قيد الشحن",
    "تم التسليم": "تم التسليم",
    "delivered": "تم التسليم",
    "مكتمل": "تم التسليم",
    "complete": "تم التسليم",
    "completed": "تم التسليم",
    "ملغي": "ملغي",
    "كنسل": "ملغي",
    "canceled": "ملغي",
    "cancelled": "ملغي",
    "مرتجع": "مرتجع",
    "returned": "مرتجع",
    "راجع": "مرتجع",
    "رجع": "مرتجع",

    # --- Payment Status ---
    "مدفوع": "مدفوع",
    "تم الدفع": "تم الدفع",
    "خالص": "تم الدفع",
    "paid": "تم الدفع",
    "معلق": "معلق",
    "قيد الانتظار": "معلق",
    "pending": "معلق",
    "مرفوض": "مرفوض",
    "rejected": "مرفوض",
    "failed": "مرفوض",
    "بانتظار الدفع": "بانتظار الدفع",
    "unpaid": "بانتظار الدفع",
    "غير مدفوع": "بانتظار الدفع",

    # --- Delivery Type ---
    "عادي": "عادي",
    "normal": "عادي",
    "standard": "عادي",
    "سريع": "سريع",
    "fast": "سريع",
    "express": "سريع",
    "فوري": "فوري",
    "instant": "فوري",
    "immediate": "فوري",
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
    text = str(original).strip()
    
    # Remove emojis or weird characters (keep Arabic letters, English letters, digits, spaces, and standard punctuation)
    cleaned = re.sub(r"[^\w\s\-\.\,\/\@\+\(\)]", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    
    # If it is a city field, standardize spelling
    if field == "city":
        cleaned = CITY_STANDARDIZATION.get(cleaned, cleaned)
        
    _record_correction(
        corrections, field, original, cleaned, "TEXT_NORMALIZATION"
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
        
    # Clean currency prefixes and suffixes dynamically
    text = re.sub(
        r"^(USD|SAR|YER|[\$])\s*|\s*(ريال يمني|ريال|لاير يمني|لاير|YER|USD|SAR|[\$])$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    
    # Clean delimiters
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
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
) -> None:
    currency = record.get("currency")
    if currency is None:
        return
    cleaned = str(currency).strip().lower()
    
    target_curr = "YER"
    rate = 1.0
    
    # Exchange rate conversion logic (USD -> YER @ 530, SAR -> YER @ 140)
    if cleaned in ("usd", "$", "dollar", "دولار"):
        target_curr = "YER"
        rate = 530.0
    elif cleaned in ("sar", "سعودي", "ريال سعودي", "سعوديه"):
        target_curr = "YER"
        rate = 140.0
    elif cleaned in ("yer", "ريال", "ريال يمني", "لاير", "لاير يمني", "يمني"):
        target_curr = "YER"
        rate = 1.0
    else:
        target_curr = str(currency).strip().upper()
        rate = 1.0
        
    if target_curr == "YER" and rate > 1.0:
        _record_correction(
            corrections, "currency", currency, "YER", "CURRENCY_CONVERSION_TO_YER"
        )
        record["currency"] = "YER"
        
        # Convert numeric fields to YER
        for field in ("delivery_cost", "payment_amount", "total_amount"):
            val = record.get(field)
            if val is not None:
                parsed = _parse_number(val, field, corrections)
                if parsed is not None:
                    converted = round(parsed * rate, 2)
                    _record_correction(
                        corrections,
                        field,
                        val,
                        converted,
                        "CURRENCY_VALUE_CONVERSION",
                    )
                    record[field] = converted
                    
        # Also convert items_json unit prices
        original_items = record.get("items_json")
        if original_items:
            try:
                items = json.loads(original_items) if isinstance(original_items, str) else original_items
                if isinstance(items, list):
                    converted_items = []
                    for index, item in enumerate(items):
                        if isinstance(item, dict):
                            qty = _parse_number(item.get("qty"), f"items[{index}].qty", corrections) or 0.0
                            unit_price = _parse_number(item.get("unit_price"), f"items[{index}].unit_price", corrections)
                            if unit_price is not None:
                                conv_price = round(unit_price * rate, 2)
                                _record_correction(
                                    corrections,
                                    f"items[{index}].unit_price",
                                    item.get("unit_price"),
                                    conv_price,
                                    "CURRENCY_VALUE_CONVERSION",
                                )
                                new_item = dict(item)
                                new_item["qty"] = qty
                                new_item["unit_price"] = conv_price
                                new_item["total"] = round(qty * conv_price, 2)
                                converted_items.append(new_item)
                            else:
                                converted_items.append(item)
                        else:
                            converted_items.append(item)
                    record["items_json"] = converted_items
            except Exception:
                pass
    else:
        # Standardize YER aliases
        if cleaned in ("yer", "ريال", "ريال يمني", "لاير", "لاير يمني", "يمني"):
            if currency != "YER":
                _record_correction(
                    corrections, "currency", currency, "YER", "CURRENCY_TO_YER"
                )
                record["currency"] = "YER"
        else:
            if currency != target_curr:
                _record_correction(
                    corrections, "currency", currency, target_curr, "CURRENCY_STANDARDIZE"
                )
                record["currency"] = target_curr


def _normalize_phone(
    record: dict[str, Any], corrections: list[dict[str, Any]]
) -> None:
    original = record.get("customer_phone")
    if original is None:
        return
    text = normalize_digits(str(original)).strip()
    compact = re.sub(r"[^\d+]", "", text)
    
    # Handle country codes
    if compact.startswith("+967"):
        compact = compact[4:]
    elif compact.startswith("00967"):
        compact = compact[5:]
    elif compact.startswith("967") and len(compact) > 9:
        compact = compact[3:]
        
    # Handle leading zero, e.g. 0771234567 -> 771234567
    if compact.startswith("0") and len(compact) == 10:
        compact = compact[1:]
    elif compact.startswith("0") and len(compact) == 9:
        compact = compact[1:]
        
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
    text = str(original).strip().strip(".")
    repaired = text.replace("@@", "@").replace("..", ".")
    
    # Correct common domain typos
    if "@" in repaired:
        local, domain = repaired.rsplit("@", 1)
        domain_lower = domain.lower()
        if domain_lower in ("gmail.co", "gamil.com", "gml.com", "gmail.com.", "gmail.co.com", "g-mail.com"):
            domain = "gmail.com"
        elif domain_lower in ("yahoo.co", "yaho.com", "yahooo.com"):
            domain = "yahoo.com"
        elif domain_lower in ("outlook.co", "outlok.com"):
            domain = "outlook.com"
        elif domain_lower in ("hotmail.co", "hotmial.com"):
            domain = "hotmail.com"
        repaired = f"{local}@{domain}"
        
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
    
    # Handle numeric epochs
    parsed: datetime | date | None = None
    if text.replace(".", "").isdigit():
        try:
            val = float(text)
            if val > 5000000000:
                val /= 1000.0
            parsed = datetime.fromtimestamp(val)
        except Exception:
            pass
            
    if parsed is None:
        text = re.sub(r"\s+", " ", text)
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%Y.%m.%d",
            "%d.%m.%Y",
        ):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
                
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            pass
            
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
            
    # Dictionary standardization using synonyms mapping
    for field in ("status", "payment_status", "delivery_type"):
        value = record.get(field)
        if value is not None:
            normalized = STATUS_ALIASES.get(str(value).strip().lower(), str(value).strip())
            _record_correction(
                corrections, field, value, normalized, "DICTIONARY_STANDARDIZATION"
            )
            record[field] = normalized

    item_total = _normalize_items(record, corrections, errors)
    
    # Inferences for missing/negative fields if math matches
    if item_total is not None:
        delivery_cost = float(record.get("delivery_cost") or 0)
        calculated_total = round(item_total + delivery_cost, 2)
        
        # Repair negative total_amount if it's a dash typo (e.g. -5000 -> 5000)
        total_val = record.get("total_amount")
        if total_val is not None:
            try:
                total_float = float(total_val)
                if total_float < 0 and abs(abs(total_float) - calculated_total) <= 0.01:
                    _record_correction(
                        corrections, "total_amount", total_val, calculated_total, "NEGATIVE_SIGN_TYPO_REPAIR"
                    )
                    record["total_amount"] = calculated_total
                    total_val = calculated_total
            except Exception:
                pass
                
        # Infill missing delivery_cost
        delivery_val = record.get("delivery_cost")
        if delivery_val is None or str(delivery_val).strip() == "":
            delivery_type = record.get("delivery_type", "")
            inferred_cost = 1000.0
            if "سريع" in delivery_type or "fast" in delivery_type.lower() or "express" in delivery_type.lower():
                inferred_cost = 2000.0
            elif "فوري" in delivery_type or "instant" in delivery_type.lower():
                inferred_cost = 3000.0
            _record_correction(
                corrections, "delivery_cost", delivery_val, inferred_cost, "INFER_DELIVERY_COST"
            )
            record["delivery_cost"] = inferred_cost
            delivery_cost = inferred_cost
            calculated_total = round(item_total + delivery_cost, 2)

        # Infill missing payment_amount
        payment_val = record.get("payment_amount")
        if (payment_val is None or str(payment_val).strip() == "") and record.get("payment_status") == "تم الدفع":
            if record.get("total_amount") is not None:
                try:
                    total_float = float(record["total_amount"])
                    _record_correction(
                        corrections, "payment_amount", payment_val, total_float, "INFER_PAYMENT_AMOUNT_FROM_TOTAL"
                    )
                    record["payment_amount"] = total_float
                except Exception:
                    pass

        # Compare total_amount vs calculated_total
        if record.get("total_amount") is None:
            errors.append(
                {
                    "code": "UNKNOWN_PRICE",
                    "detail": "Total amount is missing and cannot be safely inferred",
                }
            )
        else:
            try:
                total_amount_float = float(record["total_amount"])
                if total_amount_float < 0:
                    errors.append(
                        {
                            "code": "AMBIGUOUS_NEGATIVE_VALUE",
                            "detail": "total_amount is negative and cannot be repaired",
                        }
                    )
                elif abs(total_amount_float - calculated_total) > 0.01:
                    original_total = record["total_amount"]
                    record["total_amount"] = calculated_total
                    _record_correction(
                        corrections,
                        "total_amount",
                        original_total,
                        calculated_total,
                        "TOTAL_RECALCULATE_FROM_ITEMS_AND_DELIVERY",
                    )
            except Exception:
                errors.append(
                    {
                        "code": "UNKNOWN_PRICE",
                        "detail": "Total amount is non-numeric and cannot be repaired",
                    }
                )

    # Check for remaining negative values
    for field in ("delivery_cost", "payment_amount"):
        val = record.get(field)
        if val is not None:
            try:
                if float(val) < 0:
                    errors.append(
                        {
                            "code": "AMBIGUOUS_NEGATIVE_VALUE",
                            "detail": f"{field} is negative and its meaning is ambiguous",
                        }
                    )
            except Exception:
                pass

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
