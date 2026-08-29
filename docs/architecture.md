# Architecture

## Flow

```text
Dirty CSV
   |
   v
File Router (size <= SMALL_FILE_THRESHOLD_MB?)
   |                         |
   v                         v
Python streaming            PySpark DataFrame
   |                         |
   +------------+------------+
                v
          orders_raw
                |
                v
      Quality rules + audit trail
          |                 |
          v                 v
  orders_validated    orders_quarantine
       (upsert)       (errors + raw record)
                |
                v
       reports/results.json
```

## MongoDB consistency and smart polling

`orders_validated` receives a strict `$jsonSchema` validator and a unique
`order_id` index. Normal full-load writes use
`ReplaceOne(..., upsert=True)`. Versioned writes use a MongoDB update pipeline
that conditionally replaces the document only when the incoming version is
newer or equal, so the version check and write are one database-side decision.
An unversioned retry cannot replace an already-versioned document.
The repository also exposes server-side aggregation pipelines for error counts,
validated status counts, and latest-version-per-order selection.

Before an expensive run, `SmartPoller` consults the durable
`pipeline_source_state` and `pipeline_processing_claims` collections. It first
uses size/mtime as a cheap check, hashes only when the source may have changed,
and compares the configured numeric version watermark when available. The same
content, an active claim, or a non-newer Delta is skipped. An expired claim is
reclaimed, and its checkpoint is returned to the runner. A successful run
updates the watermark only after the ELT completes.

Each raw, validated, and quarantine batch has a deterministic Idempotency Key.
MongoDB stores the payload hash in `pipeline_idempotency_keys`; a completed
request is ignored, a processing request can be retried safely, and a changed
payload under the same key raises an explicit error. Raw and quarantine
documents also receive deterministic IDs for crash-safe `insert_many` retries.

## Design decisions

1. **Raw is append-only history.** Every run gets a `run_id`; no validator or
   unique business index blocks dirty input.
2. **Business state is keyed by `order_id`.** The validated collection has a
   unique index, and the final write uses an atomic upsert. Full loads use
   `ReplaceOne(..., upsert=True)`; versioned loads use a conditional update
   pipeline. The read used for insert/update/unchanged metrics is never used as
   a write decision.
3. **Quality rules are conservative.** A transformation is applied only when
   the rule is deterministic. Material errors are retained in quarantine instead
   of being dropped.
4. **The Spark path keeps sensitive CSV columns as strings.** It writes the raw
   DataFrame with the MongoDB Spark Connector before partition-level cleaning.
   No Pandas conversion or unreasoned `repartition` is used.
5. **The in-memory repository is an explicit test adapter.** It allows the
   quality and idempotency contract to be verified without hiding MongoDB
   connection failures in production mode.
6. **Recovery is batch-granular.** Raw loading is replay-safe, while quality
   processing checkpoints the last fully written source row. A retry may repeat
   at most one in-flight batch, and its Idempotency Keys make that repeat safe.

## Data contract

Raw documents contain `run_id`, `source_file`, `source_row_number`,
`ingested_at`, `engine_used`, and `raw_record`. Validated documents contain
the normalized business fields, `quality_status`, `corrections`, and source
trace metadata. Quarantine documents additionally contain `error_codes` and
`error_details`.
