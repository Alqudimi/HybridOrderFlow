#!/usr/bin/env python3
"""Generate a large CSV file with 1,000,000+ realistic order records.

Usage::

    python3 scripts/generate_million_records.py                     # 1M records → data/orders_1m.csv
    python3 scripts/generate_million_records.py --rows 2000000      # 2M records
    python3 scripts/generate_million_records.py --output /tmp/big.csv

The generated file mirrors the exact column schema expected by the pipeline
(``CSV_COLUMNS`` in ``spark_loader.py``), and includes a realistic mix of:
* ~70 % clean / valid records
* ~20 % records with correctable issues (Arabic digits, dirty emails, etc.)
* ~10 % records with hard errors (impossible dates, corrupt JSON, etc.)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import string
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COLUMNS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json",
]

CITIES = [
    "صنعاء", "عدن", "تعز", "الحديدة", "إب", "ذمار", "المكلا",
    "سيئون", "عمران", "حجة", "صعدة", "البيضاء", "مأرب", "لحج",
]
DISTRICTS = [
    "التحرير", "المدينة", "الصافية", "الثورة", "الوحدة",
    "النصر", "السلام", "الحرية", "الأمل", "المنصورة",
]
STATUSES = ["مؤكد", "قيد التنفيذ", "تم التسليم", "ملغي", "مرتجع"]
DELIVERY_TYPES = ["عادي", "سريع", "فوري"]
PAYMENT_METHODS = ["بطاقة", "كاش", "تحويل بنكي", "محفظة إلكترونية"]
PAYMENT_STATUSES = ["تم الدفع", "معلق", "مرفوض"]
CURRENCIES = ["YER", "ريال يمني", "SAR", "USD"]
PRODUCT_NAMES = [
    "هاتف ذكي", "لابتوب", "سماعات بلوتوث", "شاحن سريع", "حافظة هاتف",
    "ماوس لاسلكي", "لوحة مفاتيح", "كابل USB", "باور بانك", "ساعة ذكية",
    "تابلت", "كاميرا", "طابعة", "شاشة عرض", "قرص صلب خارجي",
]
ARABIC_DIGITS = str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩")

# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def _random_phone() -> str:
    prefix = random.choice(["71", "73", "77", "70", "78"])
    return prefix + "".join(random.choices("0123456789", k=7))


def _random_email(customer_id: str) -> str:
    domains = ["gmail.com", "yahoo.com", "outlook.com", "mail.com"]
    return f"{customer_id}@{random.choice(domains)}"


def _random_items(count: int = None) -> str:
    count = count or random.randint(1, 5)
    items = []
    for _ in range(count):
        qty = random.randint(1, 10)
        price = random.choice([500, 1000, 2000, 5000, 10000, 25000, 50000])
        items.append({
            "sku": f"SKU-{random.randint(1000, 9999)}",
            "name": random.choice(PRODUCT_NAMES),
            "qty": qty,
            "unit_price": price,
            "total": qty * price,
        })
    return json.dumps(items, ensure_ascii=False)


def _calculate_total(items_json_str: str, delivery_cost: float) -> float:
    items = json.loads(items_json_str)
    return sum(item["total"] for item in items) + delivery_cost


def _random_date(valid: bool = True) -> str:
    """Generate a date string. If not valid, may produce impossible dates."""
    year = random.choice([2024, 2025])
    month = random.randint(1, 12)
    day = random.randint(1, 28) if valid else random.randint(29, 31)
    # Mix formats
    fmt = random.choice([
        f"{year}-{month:02d}-{day:02d}",
        f"{day:02d}/{month:02d}/{year}",
        f"{day:02d}-{month:02d}-{year}",
    ])
    return fmt


def _generate_clean_record(index: int) -> dict[str, str]:
    """Generate a clean, valid record."""
    order_id = f"ORD-{index:08d}"
    customer_id = f"CUST-{random.randint(1, 200000):06d}"
    delivery_cost = random.choice([0, 500, 1000, 1500, 2000])
    items_json = _random_items()
    total = _calculate_total(items_json, delivery_cost)

    return {
        "order_id": order_id,
        "order_date": _random_date(valid=True),
        "status": random.choice(STATUSES),
        "customer_id": customer_id,
        "customer_name": f"عميل {random.randint(1, 100000)}",
        "customer_phone": _random_phone(),
        "customer_email": _random_email(customer_id),
        "city": random.choice(CITIES),
        "district": random.choice(DISTRICTS),
        "delivery_type": random.choice(DELIVERY_TYPES),
        "delivery_cost": str(delivery_cost),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choice(PAYMENT_STATUSES),
        "payment_amount": str(total),
        "currency": "YER",
        "total_amount": str(total),
        "items_json": items_json,
    }


def _add_correctable_noise(record: dict[str, str]) -> dict[str, str]:
    """Inject correctable issues into a record."""
    noise_type = random.choice([
        "arabic_digits", "dirty_email", "phone_spaces",
        "currency_symbol", "thousands_sep", "extra_spaces",
    ])
    r = dict(record)

    if noise_type == "arabic_digits":
        r["delivery_cost"] = r["delivery_cost"].translate(ARABIC_DIGITS)
        r["total_amount"] = r["total_amount"].translate(ARABIC_DIGITS)
    elif noise_type == "dirty_email":
        r["customer_email"] = r["customer_email"].replace("@", "@@")
    elif noise_type == "phone_spaces":
        phone = r["customer_phone"]
        r["customer_phone"] = f"+967 {phone[:3]} {phone[3:6]} {phone[6:]}"
    elif noise_type == "currency_symbol":
        r["delivery_cost"] = f"{r['delivery_cost']} ريال يمني"
        r["currency"] = "ريال يمني"
    elif noise_type == "thousands_sep":
        try:
            val = float(r["total_amount"])
            r["total_amount"] = f"{val:,.2f}"
        except ValueError:
            pass
    elif noise_type == "extra_spaces":
        r["status"] = f"  {r['status']}  "
        r["customer_name"] = f"  {r['customer_name']}  "

    return r


def _generate_hard_error_record(index: int) -> dict[str, str]:
    """Generate a record that will be quarantined."""
    r = _generate_clean_record(index)
    error_type = random.choice([
        "impossible_date", "corrupt_json", "empty_items",
        "missing_order_id", "missing_customer",
    ])

    if error_type == "impossible_date":
        r["order_date"] = "31/02/2025"
    elif error_type == "corrupt_json":
        r["items_json"] = '{"sku":"not-a-list"}'
    elif error_type == "empty_items":
        r["items_json"] = "[]"
    elif error_type == "missing_order_id":
        r["order_id"] = ""
    elif error_type == "missing_customer":
        r["customer_id"] = ""

    return r


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def generate_csv(output_path: Path, total_rows: int) -> None:
    """Write a CSV file with the specified number of rows."""
    start = time.perf_counter()
    print(f"Generating {total_rows:,} records → {output_path} ...")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()

        for i in range(1, total_rows + 1):
            roll = random.random()
            if roll < 0.70:
                row = _generate_clean_record(i)
            elif roll < 0.90:
                row = _add_correctable_noise(_generate_clean_record(i))
            else:
                row = _generate_hard_error_record(i)
            writer.writerow(row)

            if i % 100_000 == 0:
                pct = i / total_rows * 100
                elapsed = time.perf_counter() - start
                rate = i / elapsed
                print(
                    f"  [{pct:5.1f}%] {i:>10,} / {total_rows:,} "
                    f"({rate:,.0f} rows/sec, {elapsed:.1f}s)"
                )

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    elapsed = time.perf_counter() - start
    print(
        f"\nDone! {total_rows:,} records written in {elapsed:.1f}s "
        f"({file_size_mb:.1f} MB)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate large CSV dataset for pipeline benchmarking"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="Number of records to generate (default: 1,000,000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/orders_1m.csv",
        help="Output CSV file path (default: data/orders_1m.csv)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    args = parser.parse_args()

    random.seed(args.seed)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generate_csv(output_path, args.rows)


if __name__ == "__main__":
    main()
