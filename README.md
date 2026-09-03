# spark-declarative-pipelines-cdc — Debezium CDC on OSS Apache Spark

Delta Live Tables became open source: Databricks donated the framework
to Apache Spark as **Spark Declarative Pipelines (SDP)**, shipped since
Spark 4.1 with a `spark-pipelines` CLI and the same
`from pyspark import pipelines as dp` Python API used on Databricks.

This repository is a **working, verified-on-a-laptop** Debezium
(MongoDB) CDC pipeline on pure OSS Spark 4.2 + Delta Lake — bronze
streaming ingestion, a VARIANT-typed current-state table, and a typed
projection view — together with an honest write-up of every version
pin and limitation required to make it work. Verified on a MacBook Pro
(Apple Silicon M4); everything runs natively on ARM.

Companion projects (the Databricks versions this was ported from):
[debezium-mongodb-cdc-pipeline](https://github.com/morillo/debezium-mongodb-cdc-pipeline),
[debezium-cdc-pipeline](https://github.com/morillo/debezium-cdc-pipeline),
[dlt-debezium-mongodb](https://github.com/morillo/dlt-debezium-mongodb),
[dlt-debezium-mysql](https://github.com/morillo/dlt-debezium-mysql).

## Quick start

```bash
./setup.sh      # one-time: venv, pinned packages, Delta jars, Spark confs
./run.sh        # run the pipeline (or: ./run.sh dry-run to just validate)
./inspect.sh    # dump the Delta tables and assert the CDC semantics
```

Prerequisites: Java 17+ (`java -version`) and Python 3.10+ — a
pyenv-managed interpreter works fine; plain `venv` + `pip` is all the
tooling needed (no `uv` required).

## Exact versions, and why each one matters

| Component | Version | Why exactly this |
|---|---|---|
| Java | 17+ (21 OK) | Spark 4.x requirement. |
| Python | 3.10–3.12 | pyspark 4.2 support range. |
| `pyspark[connect]` | **4.2.0** | First line with `dp.create_auto_cdc_flow`; the `connect` extra (pandas, pyarrow, grpcio) is **required** — the CLI runs the pipeline through a local Spark Connect server. |
| `delta-spark` | **4.4.0** | **Trap:** Delta's versions do not track Spark's. `delta-spark==4.2.0` targets Spark **4.1** and silently downgrades your pyspark to 4.1.1 (which has no AUTO CDC API). 4.4.0 is the Spark 4.2-compatible line. |
| Delta jars | 4.4.0 (+ antlr4 4.13.1) | Must match the Python package; fetched from Maven Central by `setup.sh`. |
| `pyyaml` | any | The `spark-pipelines` CLI imports it but pip does not pull it in. |

## Findings: what is (and is not) in OSS SDP

Everything below was discovered by running, not by reading docs.

**Works out of the box:**

- `spark-pipelines init | dry-run | run` CLI, spec file + decorated
  Python transformations, streaming tables, materialized views,
  temporary views — the same programming model as Databricks.
- **VARIANT is fully OSS** (Spark 4.0+): `parse_json`,
  `try_variant_get`, `doc:path::type` — the schemaless MongoDB
  approach ports verbatim.
- Delta-backed pipeline tables via `format="delta"` on each dataset.

**Limitations (as of Spark 4.2.0 / Delta 4.4.0):**

1. **`create_auto_cdc_flow` exists but is not yet usable.** The API
   was upstreamed (SCD type 1 only — no SCD2), but the engine requires
   a DSv2 connector advertising row-level operations
   (`AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE`). Delta OSS implements
   MERGE through its own extensions, not that interface, and no
   Iceberg runtime exists for Spark 4.2 yet — so **no available
   connector satisfies it**. This repo uses the portable pattern
   instead: current state as a materialized view with
   `row_number() OVER (PARTITION BY _id ORDER BY ts_ms DESC, ord
   DESC)` — deletes filtered, latest event wins, same semantics.
2. **No expectations yet** (`dp.expect*` is absent). Quality gates
   become `.where()` filters: rows are dropped, but there are no
   event-log metrics.
3. **No Auto Loader** (`cloudFiles` is Databricks-proprietary). Use
   the native file streaming source with an explicit schema — for
   Debezium MongoDB the envelope is static, so this costs nothing.
4. **Unity Catalog / event log / serverless** are Databricks-side;
   locally you get a session catalog with Delta files under
   `pipeline/spark-warehouse/`.

**Setup gotchas the scripts already handle:**

- `SPARK_HOME` must point at the pip-installed distribution and
  `PYSPARK_PYTHON` at the venv interpreter, or the CLI's JVM launcher
  cannot find Python (`Java gateway exited`, `dirname(None)`).
- `spark.jars` / `spark.sql.extensions` are **static** confs: setting
  them in the spec's `configuration:` fails with
  `CANNOT_MODIFY_CONFIG`. They belong in
  `$SPARK_HOME/conf/spark-defaults.conf` (written by `setup.sh`).
- The spec's `storage:` must be an **absolute `file://` URI** —
  relative paths fail with `PIPELINE_STORAGE_ROOT_INVALID`. `run.sh`
  renders the committed template with the checkout's absolute path.
- Stale `spark-warehouse/` from a failed run blocks re-creation
  (`DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION`); remove
  `pipeline/spark-warehouse`, `pipeline/metastore_db`, and
  `pipeline/pipeline-storage` for a clean slate.

## Inspecting everything by hand

After `./run.sh`:

```bash
# Delta tables on disk
ls pipeline/spark-warehouse/
ls pipeline/spark-warehouse/customers_silver/_delta_log/

# Streaming checkpoints and pipeline state
ls pipeline/pipeline-storage/

# Full run log (flow-by-flow progress)
./run.sh run > pipeline/run.log 2>&1 ; grep "Flow" pipeline/run.log

# Ad-hoc SQL against the tables (Delta confs come from
# spark-defaults.conf, so a plain session can read everything)
export SPARK_HOME="$(.venv/bin/python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
.venv/bin/pyspark
>>> df = spark.read.format("delta").load("pipeline/spark-warehouse/customers_silver")
>>> df.selectExpr("_id", "doc:name::string", "doc:address.city::string").show()
```

`./inspect.sh` automates this: row counts per Debezium `op`, the
current-state table, the typed projection, and four PASS/FAIL checks
(update applied, delete applied, same-millisecond tiebreak by oplog
`ord`, non-ObjectId `_id` handled).

## Running from Visual Studio Code

1. `./setup.sh` once from any terminal, then open this directory in
   VS Code (`code .`).
2. **Interpreter**: `⇧⌘P` → *Python: Select Interpreter* → choose
   `.venv/bin/python` in this repo. You now get IntelliSense and
   go-to-definition on `pyspark.pipelines` in
   `pipeline/transformations/mongo_cdc.py`.
3. **Run**: use the integrated terminal (`` ⌃` ``): `./run.sh dry-run`
   for a fast validation loop while editing, `./run.sh` to execute,
   `./inspect.sh` to check results.
4. Optional one-keystroke build task (`⇧⌘B`) — create
   `.vscode/tasks.json`:

   ```json
   {
     "version": "2.0.0",
     "tasks": [
       {
         "label": "sdp: run pipeline",
         "type": "shell",
         "command": "./run.sh",
         "group": { "kind": "build", "isDefault": true },
         "problemMatcher": []
       },
       {
         "label": "sdp: dry-run",
         "type": "shell",
         "command": "./run.sh dry-run",
         "problemMatcher": []
       },
       {
         "label": "sdp: inspect tables",
         "type": "shell",
         "command": "./inspect.sh",
         "problemMatcher": []
       }
     ]
   }
   ```

5. Debugging note: the transformations execute inside the
   CLI-launched Spark Connect server, so VS Code's Python debugger
   does not attach to them; debug transformation logic by pasting it
   into a `.venv/bin/pyspark` session (or a notebook) against the
   bronze table.

## Layout

```
spark-declarative-pipelines-cdc/
├── setup.sh                       # venv + pinned deps + jars + confs
├── run.sh                         # render spec, launch spark-pipelines
├── inspect.sh / inspect_tables.py # dump tables, assert CDC semantics
└── pipeline/
    ├── spark-pipeline.template.yml# spec template (storage path token)
    ├── transformations/
    │   └── mongo_cdc.py           # bronze -> silver -> typed (dp API)
    └── events/
        └── mongodb.shopdb.customers/  # sample Debezium events
```

The sample events cover snapshot reads, an insert, a full-document
update, a pre-image delete, a same-millisecond update+delete pair
(resolved by oplog `ord`), a Kafka tombstone, and schema-drift
documents (ObjectId and integer `_id`s, fields present on one
document only).
