from src.quality_rules import classify_record


def valid_record() -> dict[str, str]:
    return {
        "order_id": "طلب-1",
        "order_date": "31/01/2025",
        "status": " مؤكد ",
        "customer_id": "عميل-1",
        "customer_name": " عميل ",
        "customer_phone": "+967 712 345 678",
        "customer_email": "user@@mail..com",
        "city": "صنعاء",
        "district": "التحرير",
        "delivery_type": "عادي",
        "delivery_cost": "٢٬٠٠٠ ريال يمني",
        "payment_method": "بطاقة",
        "payment_status": "تم الدفع",
        "payment_amount": "٧٠٦٠٠٠٫٠",
        "currency": "ريال يمني",
        "total_amount": "125,000.00",
        "items_json": (
            '[{"sku":"SKU-1","name":"منتج","qty":"٢",'
            '"unit_price":"خمسة آلاف","total":"0"}]'
        ),
    }


def test_applies_corrections_and_preserves_audit_trail() -> None:
    result = classify_record(valid_record())
    assert result["quality_status"] == "corrected"
    assert result["customer_email"] == "user@mail.com"
    assert result["customer_phone"] == "712345678"
    assert result["currency"] == "YER"
    assert result["order_date"] == "2025-01-31"
    assert result["items_json"][0]["qty"] == 2.0
    assert result["total_amount"] == 12_000.0
    rules = {entry["rule_code"] for entry in result["corrections"]}
    assert {
        "DATE_STANDARDIZE",
        "CURRENCY_TO_YER",
        "PHONE_REMOVE_SPACES_AND_COUNTRY_CODE",
        "EMAIL_REPEATED_SYMBOLS",
        "PRICE_KNOWN_WORD",
        "TOTAL_RECALCULATE_FROM_ITEMS_AND_DELIVERY",
    }.issubset(rules)


def test_arabic_digits_and_thousands_separators_are_numeric() -> None:
    result = classify_record(valid_record())
    assert result["delivery_cost"] == 2000.0
    assert result["payment_amount"] == 706000.0


def test_impossible_date_is_quarantined() -> None:
    record = valid_record()
    record["order_date"] = "31/02/2025"
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert "INVALID_IMPOSSIBLE_DATE" in result["error_codes"]


def test_corrupt_and_empty_items_are_quarantined() -> None:
    corrupt = valid_record()
    corrupt["items_json"] = '{"sku":"not-a-list"}'
    assert "CORRUPTED_ITEMS_JSON" in classify_record(corrupt)["error_codes"]

    empty = valid_record()
    empty["items_json"] = "[]"
    assert "EMPTY_ITEMS" in classify_record(empty)["error_codes"]


def test_negative_quantity_is_not_guessed() -> None:
    record = valid_record()
    record["items_json"] = (
        '[{"sku":"SKU-1","name":"منتج","qty":-2,"unit_price":5000}]'
    )
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert "AMBIGUOUS_NEGATIVE_VALUE" in result["error_codes"]
