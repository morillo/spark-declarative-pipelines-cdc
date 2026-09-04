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
(Apple Silicon M4); everything runs natively on ARM. Spark itself is a
shared, version-managed install (see the prerequisite below); all
project state — venv, Delta jars, confs, tables — stays inside this
directory and never touches it.

Companion projects (the Databricks versions this was ported from):
[debezium-mongodb-cdc-pipeline](https://github.com/morillo/debezium-mongodb-cdc-pipeline),
[debezium-cdc-pipeline](https://github.com/morillo/debezium-cdc-pipeline),
[dlt-debezium-mongodb](https://github.com/morillo/dlt-debezium-mongodb),
[dlt-debezium-mysql](https://github.com/morillo/dlt-debezium-mysql).

## Prerequisite: Apache Spark 4.2.0

This project **requires the official `spark-4.2.0-bin-hadoop3`
distribution**, installed wherever you keep Spark versions. One-time
install (immutable, pinned URL):

```bash
curl -fLO https://archive.apache.org/dist/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz
tar -xzf spark-4.2.0-bin-hadoop3.tgz
mv spark-4.2.0-bin-hadoop3 <your spark versions directory>/spark-4.2.0
```

The scripts default to `/usr/local/spark-versions/spark-4.2.0` and
every one honors a `SPARK_HOME` override for any other location:
`SPARK_HOME=/opt/spark-4.2.0 ./run.sh`. This install can coexist with
any other Spark versions; the project never modifies it.

Also required: Java 17+ (`java -version`) and Python 3.10+ — a
pyenv-managed interpreter works fine; plain `venv` + `pip` is all the
Python tooling needed (no `uv` required).

## Quick start

```bash
./setup.sh      # one-time: venv, Delta jars, project-local confs
./run.sh        # run the pipeline (or: ./run.sh dry-run to validate)
./inspect.sh    # dump the Delta tables and assert the CDC semantics
./shell.sh      # interactive PySpark shell with Delta configured
```

## How the shared install stays pristine

The system Spark install is **never modified**. Everything
project-specific is local to the repository and activated per run:

- **Delta jars** (`delta-spark_2.13-4.4.0`, `delta-storage-4.4.0`)
  are fetched by `setup.sh` into `./jars/` — not into the shared
  `spark-4.2.0/jars/`.
- **Confs** (`spark.jars` pointing at `./jars`, the Delta SQL
  extension, and the Delta catalog) are written to
  `./conf/spark-defaults.conf` and applied through
  **`SPARK_CONF_DIR`**, Spark's supported mechanism for an external
  conf directory. Other projects using the same Spark 4.2.0 see none
  of it.
- The **virtualenv** holds only the Python client dependencies the
  CLI and Spark Connect need: `pyyaml`, `pandas`, `pyarrow`,
  `grpcio`, `grpcio-status`, `googleapis-common-protos`, and
  `zstandard` (Spark 4.2 requires it, but only tells you at run
  time). There is **no pip `pyspark`** — the distribution provides
  the library, which avoids two documented traps: pip's broken
  4.2.0 `pyspark` shell launcher, and `delta-spark==4.2.0` silently
  downgrading pyspark to 4.1.1 (Delta's versions do not track
  Spark's; the Spark 4.2-compatible Delta line is **4.4.x**).

`env.sh` (sourced by every script) is the single place these
conventions live: `SPARK_HOME` (default
`/usr/local/spark-versions/spark-4.2.0`, overridable),
`SPARK_CONF_DIR`, and the venv interpreter for driver and executors.

## Exact versions

| Component | Version | Notes |
|---|---|---|
| Java | 17+ (21 OK) | Spark 4.x requirement. |
| Python | 3.10–3.12 | Driver and executors use the venv interpreter. |
| Apache Spark | **4.2.0** (`spark-4.2.0-bin-hadoop3`, shared install) | First line with a usable `create_auto_cdc_flow` API; ships `bin/spark-pipelines`. |
| Delta Lake jars | **4.4.0** | The Spark 4.2-compatible line (4.2.x targets Spark 4.1). |
| Python client deps | latest | See list above; `zstandard>=0.25` is mandatory. |

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

**Runtime rules the scripts already handle:**

- The spec's `storage:` must be an **absolute `file://` URI** —
  relative paths fail with `PIPELINE_STORAGE_ROOT_INVALID`. `run.sh`
  renders the committed template with the checkout's absolute path.
- `PYSPARK_PYTHON` must point at the venv interpreter so the CLI and
  executors use the environment with the client dependencies.
- Stale `spark-warehouse/` from a failed or pre-upgrade run blocks
  re-creation (`DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION`); run
  `./clean.sh` for a clean slate (it removes local pipeline state
  only — code and sample events are untouched).
- A `CANNOT_MODIFY_STATIC_CONFIG: "spark.sql.extensions"` warning at
  session creation is **expected and harmless**: the Spark Connect
  client redundantly re-applies `spark-defaults.conf` per session and
  static confs cannot be set there — the server JVM already loaded
  them at launch, which is what matters.

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

# Interactive shell: the distribution's real bin/pyspark, Delta
# configured, `spark` and `sc` ready (plus the web UI on :4040)
./shell.sh
>>> df = spark.read.format("delta").load("pipeline/spark-warehouse/customers_silver")
>>> df.selectExpr("_id", "doc:name::string", "doc:address.city::string").show()

# Or plain SQL
source env.sh && "$SPARK_HOME/bin/spark-sql" -e "SELECT count(*) FROM delta.\`$PWD/pipeline/spark-warehouse/customers_bronze\`"
```

`./inspect.sh` automates this via `spark-submit`: row counts per
Debezium `op`, the current-state table, the typed projection, and four
PASS/FAIL checks (update applied, delete applied, same-millisecond
tiebreak by oplog `ord`, non-ObjectId `_id` handled).

## Running from Visual Studio Code

1. `./setup.sh` once from any terminal, then open this directory in
   VS Code (`code .`).
2. **Interpreter**: `⇧⌘P` → *Python: Select Interpreter* → choose
   `.venv/bin/python`. `setup.sh` also wrote a `.env` file pointing
   `PYTHONPATH` at the shared install's `python/` directory, which the
   Python extension reads automatically — so Pylance resolves
   `pyspark.pipelines` from Spark 4.2.0 and you get
   IntelliSense/go-to-definition in
   `pipeline/transformations/mongo_cdc.py` without pip-installing
   pyspark.
3. **Run**: integrated terminal (`` ⌃` ``): `./run.sh dry-run` for a
   fast validation loop while editing, `./run.sh` to execute,
   `./inspect.sh` to check results, `./shell.sh` to poke at tables.
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
   does not attach to them; debug transformation logic in a
   `./shell.sh` session against the bronze table.

## Layout

```
spark-declarative-pipelines-cdc/
├── setup.sh                       # venv + Delta jars + project confs
├── env.sh                         # SPARK_HOME / SPARK_CONF_DIR conventions
├── run.sh                         # render spec, launch spark-pipelines
├── shell.sh                       # shared install's bin/pyspark, Delta ready
├── inspect.sh / inspect_tables.py # dump tables, assert CDC semantics
├── clean.sh                       # wipe local pipeline state
└── pipeline/
    ├── spark-pipeline.template.yml# spec template (storage path token)
    ├── transformations/
    │   └── mongo_cdc.py           # bronze -> silver -> typed (dp API)
    └── events/
        └── mongodb.shopdb.customers/  # sample Debezium events
```

`jars/`, `conf/`, `.venv/`, `.env`, and all pipeline state are created
by the scripts and gitignored; the shared Spark install is read-only
to this project.

The sample events cover snapshot reads, an insert, a full-document
update, a pre-image delete, a same-millisecond update+delete pair
(resolved by oplog `ord`), a Kafka tombstone, and schema-drift
documents (ObjectId and integer `_id`s, fields present on one
document only).
