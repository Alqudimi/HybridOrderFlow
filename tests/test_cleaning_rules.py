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


def test_advanced_cleaning_rules() -> None:
    # 1. Test USD/SAR Currency Conversion to YER
    record_usd = valid_record()
    record_usd["currency"] = "USD"
    record_usd["total_amount"] = "20"
    record_usd["payment_amount"] = "20"
    record_usd["delivery_cost"] = "0"
    record_usd["items_json"] = '[{"sku":"SKU-1","name":"item","qty":2,"unit_price":10}]'
    
    res = classify_record(record_usd)
    assert res["quality_status"] == "corrected"
    assert res["currency"] == "YER"
    # 20 USD * 530 = 10600 YER
    assert res["total_amount"] == 10600.0
    assert res["payment_amount"] == 10600.0
    assert res["items_json"][0]["unit_price"] == 5300.0  # 10 USD * 530 = 5300 YER

    # 2. Test Smart Date Formats & Epoch Timestamps
    record_date = valid_record()
    record_date["order_date"] = "31-12-2024 15:30:45"
    res_date = classify_record(record_date)
    assert res_date["quality_status"] == "corrected"
    assert res_date["order_date"] == "2024-12-31"

    # Epoch millisecond timestamp
    record_epoch = valid_record()
    record_epoch["order_date"] = "1735689600000"  # 2025-01-01 00:00:00 UTC
    res_epoch = classify_record(record_epoch)
    assert res_epoch["quality_status"] == "corrected"
    assert res_epoch["order_date"].startswith("2025-01-01")

    # 3. Test Email Domain Repair (obvious typos)
    record_email = valid_record()
    record_email["customer_email"] = "test@gamil.com."
    res_email = classify_record(record_email)
    assert res_email["quality_status"] == "corrected"
    assert res_email["customer_email"] == "test@gmail.com"

    # 4. Test Advanced Phone Formatting
    record_phone = valid_record()
    record_phone["customer_phone"] = "0771234567"
    res_phone = classify_record(record_phone)
    assert res_phone["quality_status"] == "corrected"
    assert res_phone["customer_phone"] == "771234567"

    # Starting with 967 without country prefix
    record_phone2 = valid_record()
    record_phone2["customer_phone"] = "967771234567"
    res_phone2 = classify_record(record_phone2)
    assert res_phone2["quality_status"] == "corrected"
    assert res_phone2["customer_phone"] == "771234567"

    # 5. Test Negative Sign Typo Repair for total_amount
    record_neg = valid_record()
    record_neg["total_amount"] = "-12000"  # dash typo, absolute matches items + delivery
    res_neg = classify_record(record_neg)
    assert res_neg["quality_status"] == "corrected"
    assert res_neg["total_amount"] == 12000.0

    # 6. Test City Name Spelling Standardization
    record_city = valid_record()
    record_city["city"] = "صنعا"
    res_city = classify_record(record_city)
    assert res_city["city"] == "صنعاء"


def test_business_key_inferences() -> None:
    # Case 1: customer_id is missing, infer from order_id
    record = valid_record()
    record["customer_id"] = ""
    record["order_id"] = "طلب-100006"
    res = classify_record(record)
    assert res["quality_status"] == "corrected"
    assert res["customer_id"] == "عميل-6"

    # Case 2: order_id is missing, infer from customer_id
    record2 = valid_record()
    record2["order_id"] = ""
    record2["customer_id"] = "عميل-6"
    res2 = classify_record(record2)
    assert res2["quality_status"] == "corrected"
    assert res2["order_id"] == "طلب-100006"

    # Case 3: Standardization of format (e.g. spaces instead of dashes)
    record3 = valid_record()
    record3["order_id"] = "طلب 100021"
    record3["customer_id"] = "عميل 21"
    res3 = classify_record(record3)
    assert res3["quality_status"] == "corrected"
    assert res3["order_id"] == "طلب-100021"
    assert res3["customer_id"] == "عميل-21"


def test_extra_treatments_and_libraries() -> None:
    # 1. Customer name title removal
    record = valid_record()
    record["customer_name"] = "الدكتور محمد علي"
    res = classify_record(record)
    assert res["customer_name"] == "محمد علي"

    record_eng = valid_record()
    record_eng["customer_name"] = "Dr. John Doe"
    res_eng = classify_record(record_eng)
    assert res_eng["customer_name"] == "John Doe"

    # 2. Payment Method standardization
    record_pm = valid_record()
    record_pm["payment_method"] = "cash"
    res_pm = classify_record(record_pm)
    assert res_pm["payment_method"] == "نقدًا عند التسليم"

    record_pm2 = valid_record()
    record_pm2["payment_method"] = "كريمي"
    res_pm2 = classify_record(record_pm2)
    assert res_pm2["payment_method"] == "محفظة إلكترونية"

    # 3. Fuzzy date parsing via dateutil
    record_date = valid_record()
    record_date["order_date"] = "24 Feb 2025 21:29:00"
    res_date = classify_record(record_date)
    assert res_date["order_date"] == "2025-02-24"

    # 4. Phonenumbers standardizer
    record_phone = valid_record()
    record_phone["customer_phone"] = "+967 77 123 4567"
    res_phone = classify_record(record_phone)
    assert res_phone["customer_phone"] == "771234567"



