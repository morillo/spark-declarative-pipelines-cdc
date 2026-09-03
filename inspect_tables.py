"""Inspect the Delta tables produced by the local SDP run.

The pipeline registers its tables in a session-scoped catalog, so
after the run they are read here directly from the Delta directories
under pipeline/spark-warehouse/ (spark-defaults.conf written by
setup.sh provides the Delta jars and extensions).
"""

from pyspark.sql import SparkSession

WAREHOUSE = "pipeline/spark-warehouse"

spark = SparkSession.builder.appName("inspect-cdc").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def delta(name):
    return spark.read.format("delta").load(f"{WAREHOUSE}/{name}")


print("=" * 64)
print("customers_bronze — raw Debezium events")
bronze = delta("customers_bronze")
print(f"  {bronze.count()} events (includes the Kafka tombstone)")
bronze.select("payload.op", "payload.ts_ms").groupBy("op").count().show()

print("=" * 64)
print("customers_silver — current state, one row per _id, doc VARIANT")
delta("customers_silver").selectExpr(
    "_id", "substr(to_json(doc), 1, 80) AS doc"
).orderBy("_id").show(truncate=False)

print("=" * 64)
print("customers_typed — typed projection over the VARIANT document")
delta("customers_typed").orderBy("_id").show(truncate=False)

print("=" * 64)
print("CDC semantics checks")
silver = delta("customers_silver")
ids = {r._id for r in silver.select("_id").collect()}
email = (
    silver.where("_id = '65f1a2b3c4d5e6f7a8b9c001'")
    .selectExpr("doc:email::string AS e")
    .first()
    .e
)
checks = [
    ("update applied (Alice has new email)", email == "alice.new@example.com"),
    ("delete applied (Carol gone)", "65f1a2b3c4d5e6f7a8b9c003" not in ids),
    ("same-ms tiebreak by oplog ord (Bob gone)",
     "65f1a2b3c4d5e6f7a8b9c002" not in ids),
    ("integer _id document kept (Dave)", "42" in ids),
]
for label, ok in checks:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
