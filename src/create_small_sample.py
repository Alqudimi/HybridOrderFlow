from __future__ import annotations

import argparse
import csv
from pathlib import Path


def create_small_sample(
    input_path: str | Path, output_path: str | Path, rows: int
) -> int:
    if rows <= 0:
        raise ValueError("rows must be positive")
    input_file = Path(input_path)
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with input_file.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        header = next(reader, None)
        if header is None:
            raise ValueError("Input CSV has no header")
        with output_file.open("w", encoding="utf-8", newline="") as target:
            writer = csv.writer(target)
            writer.writerow(header)
            written = 0
            for row in reader:
                if written >= rows:
                    break
                writer.writerow(row)
                written += 1
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a reproducible streaming sample from a dirty CSV."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="data/orders_sample.csv")
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()
    written = create_small_sample(args.input, args.output, args.rows)
    print(f"created {args.output}: {written} data rows")


if __name__ == "__main__":
    main()
