"""MongoDB Debezium CDC on OSS Spark Declarative Pipelines (local).

A local adaptation of the debezium-mongodb-cdc-pipeline framework
proving the architecture runs on Apache Spark 4.2's `spark-pipelines`
CLI on a laptop. Differences from the Databricks version:

- Auto Loader (`cloudFiles`) is Databricks-only -> native JSON file
  streaming source with the same static envelope schema.
- Expectations are not yet in OSS SDP -> quality gates are `where`
  filters (rows are dropped but not metered).
- OSS `create_auto_cdc_flow` supports SCD type 1 only.
"""

import os

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

spark = SparkSession.getActiveSession()

EVENTS_PATH = "file://" + os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "events",
    "mongodb.shopdb.customers",
)

SOURCE_SCHEMA = StructType(
    [
        StructField("ts_ms", LongType(), True),
        StructField("db", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("ord", LongType(), True),
    ]
)

ENVELOPE = StructType(
    [
        StructField(
            "payload",
            StructType(
                [
                    StructField("before", StringType(), True),
                    StructField("after", StringType(), True),
                    StructField("source", SOURCE_SCHEMA, True),
                    StructField("op", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                ]
            ),
            True,
        ),
        StructField("schema", StringType(), True),
    ]
)

DOC_ID_EXPR = (
    "coalesce("
    "try_variant_get(doc, '$._id[\"$oid\"]', 'string'), "
    "try_variant_get(doc, '$._id', 'string'), "
    "to_json(try_variant_get(doc, '$._id'))"
    ")"
)


@dp.table(
    comment="Raw Debezium CDC events (native JSON stream).",
    format="delta",
)
def customers_bronze():
    return (
        spark.readStream.format("json").schema(ENVELOPE).load(EVENTS_PATH)
    )


# OSS Spark 4.2 ships create_auto_cdc_flow, but it requires a DSv2
# connector advertising row-level operations, which neither Delta OSS
# nor Iceberg satisfies yet on this Spark version. The portable OSS
# pattern is a materialized view: latest event per _id wins (sequenced
# by ts_ms with the oplog ordinal as tiebreaker), deletes filtered out.
@dp.materialized_view(
    comment="Current state: one row per _id, document as VARIANT.",
    format="delta",
)
def customers_silver():
    events = spark.read.table("customers_bronze").select("payload.*")
    parsed = events.select(
        F.col("op").alias("_cdc_op"),
        F.col("ts_ms").alias("_cdc_ts_ms"),
        F.col("source.ord").alias("_cdc_ord"),
        F.expr(
            "parse_json(CASE WHEN op = 'd' THEN before ELSE after END)"
        ).alias("doc"),
    ).withColumn("_id", F.expr(DOC_ID_EXPR))
    parsed = parsed.where("_cdc_op IS NOT NULL AND _id IS NOT NULL")
    latest = parsed.withColumn(
        "_rn",
        F.expr(
            "row_number() OVER (PARTITION BY _id "
            "ORDER BY _cdc_ts_ms DESC, _cdc_ord DESC)"
        ),
    ).where("_rn = 1 AND _cdc_op != 'd'")
    return latest.select("_id", "doc")


@dp.materialized_view(
    comment="Typed projection over the VARIANT doc.",
    format="delta",
)
def customers_typed():
    return spark.read.table("customers_silver").selectExpr(
        "_id",
        "doc:name::string AS name",
        "doc:email::string AS email",
        "doc:address.city::string AS city",
        "doc:loyalty_tier::string AS loyalty_tier",
    )
