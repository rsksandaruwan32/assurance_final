# Assurance Final

A Python learning project focused on data quality, historical data modeling, financial reconciliation, and pipeline recovery.

I made this as a test project to practise data engineering concepts and understand how a pipeline can handle duplicate events, corrections, invalid records, and processing failures.

The local application uses Python and SQLite. The same source file also contains an optional Spark and Delta implementation, which has not been validated in Databricks as part of the results documented here.

## Project overview

The application generates synthetic customer and order events, validates the input, reconstructs the latest order state, and produces financial aggregates.

The sample data includes:

1. Customer segment history.
2. New orders.
3. An order correction.
4. An order deletion.
5. A duplicate event.
6. An invalid amount.
7. An unsupported currency.
8. An invalid customer identifier.

The project also includes automated tests for failure recovery, conflicting source versions, financial reconciliation, backup restoration, and stored data corruption.

All generated business data is synthetic.

## Verified local results

I ran the local application on Windows using Python 3.11.9.

| Check | Observed result |
|---|---|
| Python syntax compilation | Passed |
| Automated tests | 25 passed |
| Generated input records | 1,027 |
| Accepted unique events | 1,023 |
| Rejected unique records | 3 |
| Duplicate events | 1 |
| Source manifest verification | Passed |
| Active orders | 999 |
| Financial control groups | 5 |
| Expected and actual financial controls | Matched in all five groups |
| Replaying the same delivery | Returned `SKIPPED` |
| Database integrity verification | Returned `ok` |
| Stored financial reconciliation | Returned `ok` |
| HTML and CSV output generation | Completed |
| Local backup creation | Completed |

The test suite includes backup restoration and failure recovery checks. These results describe local functional testing, not production performance or cloud deployment.

## How it works

```text
Synthetic customer and order events
                 |
                 v
JSON parsing and contract validation
                 |
                 +----> Rejected record diagnostics
                 |
                 v
Accepted event history
                 |
                 v
Customer history and latest order state
                 |
                 v
Aggregates by date, currency and segment
                 |
                 v
Independent financial reconciliation
                 |
                 v
Commit the local snapshot
                 |
                 v
HTML report, CSV exports and backup
```

### Delivery model

Each delivery must contain a complete event history snapshot, including the customer history and order versions required to reconstruct the result.

This is not an incremental database CDC connector.

A successful new delivery replaces the current local snapshot in one database transaction. Execution history and committed batch records are retained.

Replaying a committed batch skips processing after rechecking the source and any supplied manifest controls. It does not restore that older batch as the current snapshot.

### Event identity and versions

A repeated event identifier with the same canonical JSON payload is treated as a duplicate.

Reusing an event identifier with changed content fails the delivery.

Source versions determine the latest order state rather than the order in which events appear in the input file.

Different event identifiers for the same entity and source version are treated as an ambiguous conflict.

### Customer history

Customer segment changes are represented as historical intervals.

Orders are attributed to the customer segment that applies at the timestamp of their latest version. Customer effective timestamps must increase with source versions.

This is the reporting definition used by the project. Other systems may need separate transaction, correction, effective, and ingestion timestamps.

### Financial output

The gold output contains one row per:

```text
UTC date + currency + customer segment
```

Its measures are:

1. Active order count.
2. Revenue in integer cents.

Deleted orders are excluded.

Currencies are reported separately. The project does not perform exchange rate conversion or combine different currencies into one revenue total.

## Reconciliation

The local result is checked using separate calculation paths:

1. SQL builds the gold table.
2. A Python reference calculation derives expected totals from accepted events.
3. The generated source manifest provides counts and financial controls calculated from the synthetic scenario.

Expected and actual values are compared separately for each date, currency, and segment. A mismatch prevents publication.

These checks help identify transformation errors, but they do not establish that every real source delivery has arrived.

The generated manifest is a test control document, not an independent financial audit. A real implementation would need controls from a trusted source.

The input fingerprint uses SHA-256. It detects a mismatch against the expected file but is not a digital signature.

## Requirements

For the local application:

1. Python 3.11.
2. A terminal.
3. Write access to the output folder.

VS Code is optional and was used for the documented local run.

The local application uses the Python standard library. No third party packages are required for the local commands.

The optional Spark mode requires a separate compatible Spark and Delta environment.

## Repository structure

```text
.
├── assurance_final.py
├── README.md
└── .gitignore
```

The application creates output folders when it runs. Generated data, reports, diagnostics, databases, and backups are excluded from version control by the provided Git ignore rules.

## Getting started on Windows

Run the following commands from the folder containing `assurance_final.py`.

### 1. Create a virtual environment

```powershell
python -m venv .venv
```

Check its Python version:

```powershell
.\.venv\Scripts\python.exe --version
```

The documented local run used Python 3.11.9.

The commands below call the environment's Python executable directly, so activation is not required.

### 2. Check the syntax

```powershell
.\.venv\Scripts\python.exe -m py_compile assurance_final.py
```

A successful syntax check normally produces no output.

### 3. Run the tests

```powershell
.\.venv\Scripts\python.exe assurance_final.py test
```

Expected final result:

```text
Ran 25 tests in ...

OK
```

Execution time will vary.

### 4. Generate the sample data

```powershell
.\.venv\Scripts\python.exe assurance_final.py generate --rows 1000
```

Expected output:

```text
Generated records: 1027
```

This creates:

```text
assurance_output_final/
├── events.jsonl
└── events.jsonl.manifest.json
```

The additional records include customer history, corrections, a deletion, a duplicate, and deliberately invalid records.

Do not edit the generated input before processing it with its manifest, because the manifest includes the input fingerprint.

### 5. Process the delivery

```powershell
.\.venv\Scripts\python.exe assurance_final.py run --batch delivery1 --manifest assurance_output_final/events.jsonl.manifest.json
```

On a fresh run, the response should include:

```json
{
  "status": "SUCCESS",
  "lines": 1027,
  "accepted": 1023,
  "rejected": 3,
  "duplicates": 1,
  "manifest_verified": true,
  "active_orders": 999,
  "control_groups": 5
}
```

The full response also contains a generated run identifier, elapsed time, and report location.

### 6. Open the HTML report

```powershell
$latest = Get-Content ".\assurance_output_final\custom\reports\latest.json" -Raw | ConvertFrom-Json

Start-Process -FilePath $latest.report
```

The report contains:

1. Financial aggregates.
2. Reconciliation controls.
3. Execution history.
4. Customer history.
5. Quarantined records.

Each HTML section previews up to 100 rows. The CSV files contain the exported records.

### 7. Inspect the CSV files

```powershell
$reportFolder = Split-Path -Path $latest.report -Parent

Import-Csv (Join-Path $reportFolder "gold.csv") | Format-Table -AutoSize

Import-Csv (Join-Path $reportFolder "controls.csv") | Format-Table -AutoSize

Import-Csv (Join-Path $reportFolder "quarantine.csv") | Format-Table -AutoSize
```

In `controls.csv`, the expected order counts and amounts should match their corresponding actual values.

The column names are not commands to enter into PowerShell.

### 8. Check replay behavior

Run the same delivery again:

```powershell
.\.venv\Scripts\python.exe assurance_final.py run --batch delivery1 --manifest assurance_output_final/events.jsonl.manifest.json
```

Expected response includes:

```json
{
  "status": "SKIPPED",
  "source_controls_revalidated": true
}
```

### 9. Verify the stored result

```powershell
.\.venv\Scripts\python.exe assurance_final.py verify
```

Expected result for the default sample:

```json
{
  "integrity": "ok",
  "reconciliation": "ok",
  "control_groups": 5
}
```

### 10. Create a backup

```powershell
.\.venv\Scripts\python.exe assurance_final.py backup
```

The command prints the backup location.

Backups are local database copies, not an offsite disaster recovery solution.

## Other commands

### Complete automatic demonstration

```powershell
.\.venv\Scripts\python.exe assurance_final.py
```

This runs the tests and creates a separate demonstration folder. It includes an intentional failure and retry, followed by verification, reporting, and backup creation.

An intentional failure message is expected during the demonstration.

### Larger sample

```powershell
.\.venv\Scripts\python.exe assurance_final.py demo --rows 10000
```

This is a larger local demonstration, not a distributed scale benchmark.

### Rebuild reports

```powershell
.\.venv\Scripts\python.exe assurance_final.py report
```

Reports are static exports. After generating a new report, reload `latest.json` and reopen the report to view the updated execution history.

### Show available options

```powershell
.\.venv\Scripts\python.exe assurance_final.py --help
```

## Failure injection

The application supports three deliberate faults:

| Option | Purpose |
|---|---|
| `after_model` | Fail after rebuilding the local models |
| `after_controls` | Fail after reconciliation but before commit |
| `corrupt_gold` | Change gold revenue so reconciliation should fail |

Use a fresh output folder for a failure drill. An already committed batch may be skipped before the fault is reached.

Example:

```powershell
$recoveryHome = "assurance_output_final/recovery_" + (Get-Date -Format "yyyyMMdd_HHmmss")

.\.venv\Scripts\python.exe assurance_final.py run --home $recoveryHome --batch recovery1 --manifest assurance_output_final/events.jsonl.manifest.json --fail-at after_model
```

The first command should fail intentionally.

Retry without the fault:

```powershell
.\.venv\Scripts\python.exe assurance_final.py run --home $recoveryHome --batch recovery1 --manifest assurance_output_final/events.jsonl.manifest.json
```

Verify the result:

```powershell
.\.venv\Scripts\python.exe assurance_final.py verify --home $recoveryHome
```

These commands provide a reproducible exercise. The local verification table does not claim that every optional command or integration has been manually exercised.

## Optional project templates

The application can generate a README, Git ignore file, GitHub Actions workflow, and Databricks job template.

To inspect these without conflicting with existing project files, use a separate empty folder:

```powershell
.\.venv\Scripts\python.exe assurance_final.py assets --home generated_assets
```

The generator refuses to overwrite existing project assets.

The GitHub workflow is configured to run the tests on Windows and Linux with Python 3.11. Its presence does not mean that a GitHub Actions run has completed.

The Databricks configuration is a template. It does not deploy a job or create cloud resources.

## Optional Spark and Delta implementation

The source file includes a `spark` command with code intended to:

1. Validate a bounded input snapshot.
2. Build customer history using Spark window functions.
3. Select the latest order versions.
4. Perform temporal joins and aggregation.
5. Write managed Delta tables.
6. Read persisted gold output back from storage.
7. Reconcile the stored result against the Python reference.
8. Check the persisted bronze count.
9. Write controls and append a publication record.

This path has not been validated in Databricks as part of the documented local results.

It requires compatible compute, an authorized catalog and schema, and input paths readable by the driver. It is not expected to run in the standard library only Windows environment.

### Publication assumptions

The cloud implementation writes separate tables for each run and then appends a record identifying the completed outputs.

Consumers must select a committed run and use its table mapping. They must not select staged tables based on their names or creation order.

The design assumes:

1. One authorized publisher.
2. No modification of published run tables.
3. Appropriate permissions configured outside the script.
4. A controlled cleanup process for unpublished tables.

The script does not enforce these permissions.

Failed cloud runs can leave unpublished tables. Automatic cleanup and long term production table lifecycle management are not implemented.

## Current limitations

The application is deliberately bounded.

| Limit | Value |
|---|---:|
| Input file size | 64 MiB |
| Individual record size | 64 KiB |
| Nonempty input records | 50,000 |
| Synthetic base orders generated by the CLI | 200 to 20,000 |

The loader and reference calculation keep state in memory.

The optional Spark path uses Spark for transformations after driver side validation. It is not a distributed ingestion implementation.

The local application replaces complete snapshots and rebuilds derived tables. It is not optimized for large historical datasets.

The project does not currently implement:

1. Authentication or role based access control.
2. Tenant isolation.
3. Application managed encryption.
4. Cloud networking or secret management.
5. Automated retention or production alerting.
6. Automatic database schema migration.
7. Azure Data Factory orchestration.
8. Fabric or Synapse integration.

Use synthetic data. Diagnostics and exports can contain source values.

A hard process termination can leave a run marked `RUNNING`. Automatic reconciliation of abandoned runs is not implemented.

Reports are generated after the database transaction commits. A report generation failure does not necessarily mean the data commit failed. Reports can be regenerated separately.

## Future improvements

Areas for further work include:

1. Validating the optional Spark and Delta implementation.
2. Running and reviewing the generated CI workflow.
3. Improving input processing while preserving validation checks.
4. Expanding concurrency and process interruption tests.
5. Separating transaction, correction, and ingestion timestamps.
6. Improving cloud publication and cleanup procedures.

These are planned improvements, not completed features.

## AI assistance

I used AI tools to help develop and document this learning project. The local verification results and current limitations are described above.

## Feedback and issue reports

Feedback is valuable and appreciated. Suggestions, corrections, and questions are welcome, whether they concern the code, tests, documentation, or underlying approach.

If something does not work as expected, please include the following details where possible:

1. Your operating system and version.
2. Your Python version.
3. The exact command you ran.
4. The complete error message or traceback.
5. What you expected to happen and what happened instead.
6. Any changes you made to the code or configuration.
7. Relevant package versions if you tried the optional Spark mode, including PySpark, Delta Lake, and the Databricks Runtime version where applicable.

Check the Python version used by the local environment with:

```powershell
.\.venv\Scripts\python.exe --version
```

If you used a different interpreter, include the version and executable you actually ran.

Please also mention whether the problem occurred in the local application or the optional Spark implementation.

Before sharing logs or screenshots, remove credentials, personal information, and confidential data. A small synthetic example is preferable to real source data.

You do not need to have a proposed fix to report a problem. A clear description is helpful, and suggestions for making the project easier to understand or run are equally welcome.

Thank you for taking the time to review the project and help improve it.
