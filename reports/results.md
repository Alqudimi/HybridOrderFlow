# Results and interpretation

## Reproducible Python Batch run

The attached sample contains 10,000 data rows and is approximately 4.17 MB.
With the documented 200 MB threshold, the Router correctly selects
`python_batch`. The run is streaming and uses configurable batches; the
generated JSON file is the source of truth for the exact timing.

The observed full-sample run produced:

- `raw_loaded`: 10,000
- `valid_count`: 0
- `corrected_count`: 8,916
- `quarantine_count`: 1,084
- consistency: `10,000 = 0 + 8,916 + 1,084`

The large corrected share is expected for this deliberately mixed-quality
sample: the input contains formatting variations and deterministic total
recalculations. Quarantined rows are retained with their error codes; they are
not silently discarded.

## Python Batch vs PySpark

| Dimension | Python Batch | PySpark |
|---|---|---|
| Router selection | Files at or below 200 MB | Files above 200 MB |
| Input API | `csv.DictReader` streaming | Spark DataFrame API |
| Schema | Raw values remain strings | Fixed all-string `StructType` |
| Parallelism | Configurable `insert_many` batches | Spark input partitions |
| MongoDB raw write | MongoDB driver | MongoDB Spark Connector |
| Quality execution | Local batch loop after Raw | `mapPartitions` after Raw |
| Memory behavior | Never loads the full CSV | Distributed DataFrame execution |

The sample is intentionally below the threshold, so the normal automatic
execution exercises Python Batch. The PySpark route is implemented and can be
demonstrated with `--force-engine pyspark` or a file above the threshold. It
requires Java 11+, PySpark, MongoDB, and a compatible MongoDB Spark Connector.
The current Replit runtime does not provide Java/Spark services, so no false
Spark timing or Cluster UI evidence is claimed here; the README documents the
exact cluster command and evidence required for the practical presentation.

## Idempotency evidence

The test suite runs the same full input twice against the repository adapter:

- Raw grows by one historical set per `run_id`, as designed.
- `orders_validated` keeps one record per `order_id`.
- The second run reports unchanged records and does not increase the business
  record count.
- The Delta test proves Insert, replay unchanged, and version-aware updates.
