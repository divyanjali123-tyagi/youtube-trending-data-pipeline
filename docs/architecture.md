# Design decisions

## Why Medallion (Bronze → Silver → Gold)?

| Layer | Contract | Why it matters |
|---|---|---|
| Bronze | Exact API payloads + ingestion metadata, append-only | Lets you **replay** history after a bug fix or schema change, and gives an audit trail of what the API actually returned. |
| Silver | Typed, validated, de-duplicated, enriched; one row per video/region/run | A single trustworthy source that every downstream table is built from. |
| Gold | Aggregated, query-shaped tables | Fast, cheap Athena queries; business logic lives in one place. |

## Incremental and idempotent processing

* The ingest Lambda derives `ingest_date`/`ingest_hour` from the **Step Functions execution start time**, and the run id from the execution name. Every downstream step processes exactly those partitions.
* Glue writes use `partitionOverwriteMode=dynamic`, so a re-run replaces only its own partitions and never duplicates data.
* Gold is recomputed for the whole `trending_date` from all of that day's Silver snapshots. Intraday metrics such as view gain and snapshot counts therefore stay correct however many times the pipeline runs.

## Partition projection instead of crawlers

Every Glue table declares `projection.*` properties (date range for `trending_date`, enums for `ingest_hour`, `region` and `suite`). Athena works out partition locations from these, so:

* no Glue crawler runs and no `MSCK REPAIR TABLE` / `ALTER TABLE ADD PARTITION` is needed;
* new data is queryable as soon as Glue writes it;
* the schemas are version-controlled in `template.yaml`. A test checks that they match the Spark output exactly.

## Data quality strategy

* **Prevent**: explicit read schemas in the Glue job, so a new or changed API field can't silently change the schema.
* **Isolate**: row-level failures go to a quarantine table with a reason and the raw record, instead of being dropped or failing the job.
* **Detect**: dataset-level SQL suites after each layer: completeness (row counts, region coverage), uniqueness, validity (ranges, non-negative), consistency (likes ≤ views) and reconciliation (Gold ↔ Silver).
* **Act**: critical failures stop the state machine before bad data reaches Gold (or before a bad Gold build counts as done) and send an SNS alert. Warnings are alerted without blocking.
* **Observe**: every check result lands in the `dq_results` table, so quality can be tracked over time in Athena.

The rules use portable ANSI SQL, so the same rule file runs on Athena (Trino) in AWS and on Spark SQL in local runs and tests.

## Failure handling

| Failure | Handling |
|---|---|
| Transient YouTube API errors (429/5xx/network) | Exponential backoff, 4 attempts |
| One region fails (e.g. invalid region) | Isolated. The other regions continue; region coverage is enforced by DQ |
| YouTube quota exhausted | Run aborts immediately (retrying can't help) → SNS alert |
| Lambda service errors | Step Functions retry with backoff |
| Glue concurrent-run limit | Step Functions retry (60 s, ×2 backoff) |
| Any unhandled error | `Catch` → `NotifyPipelineError` (SNS) → `Fail`, plus a CloudWatch alarm on `ExecutionsFailed` |

## Security

* The YouTube API key is stored in **SSM Parameter Store (SecureString)**, never in code or in the template.
* IAM follows least privilege: SAM policy templates are scoped to the bucket, workgroup, functions and jobs.
* The S3 bucket blocks public access, is encrypted (SSE-S3), denies non-TLS requests and is versioned. Athena results are encrypted and the workgroup has a 1 GB per-query scan limit.
