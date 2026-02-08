import json
import os
from typing import Any, Dict, List

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import expr


def _load_json(path: str) -> Dict[str, Any]:
    # In pipelines, __file__ is available for python files (unlike notebooks).
    if not os.path.isabs(path):
        here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.abspath(os.path.join(here, path))
    with open(path, "r", encoding="utf-8") as f:
        return json.loads(f.read())


SOURCES_CONFIG_PATH = spark.conf.get("sources_config_path")
DQ_RULES_PATH = spark.conf.get("dq_rules_path")

cfg = _load_json(SOURCES_CONFIG_PATH)
dq_cfg = _load_json(DQ_RULES_PATH)

default_required_cols: List[str] = dq_cfg.get("defaults", {}).get("required_columns", [])
source_overrides: Dict[str, Any] = dq_cfg.get("sources", {}) or {}


def _build_rules_for_source(source_key: str, table_cols: List[str]) -> Dict[str, str]:
    src_required = source_overrides.get(source_key, {}).get("required_columns", [])
    required = list(dict.fromkeys(default_required_cols + src_required))  # stable ordering, de-dupe

    rules = {}
    for c in required:
        if c in table_cols:
            rules[f"{c}_not_null"] = f"({c} IS NOT NULL)"

    # Always have at least 1 rule so the quarantine pattern behaves predictably.
    if not rules:
        rules = {"always_true": "(1 = 1)"}

    return rules


sources = cfg.get("sources", [])
if not sources:
    raise ValueError("No sources found in sources.json under key: sources[]")

for s in sources:
    source_key = s.get("key")
    args = (s.get("args") or {})
    raw_table = args.get("raw_table")
    if not source_key or not raw_table:
        raise ValueError(f"Each source must have 'key' and args.raw_table. Bad entry: {s}")

    # Table names (live in the pipeline's default catalog/schema)
    bronze_name = f"bronze_{source_key}"
    quarantine_stage_name = f"_{source_key}_silver_quarantine_stage"
    silver_name = f"silver_{source_key}"
    silver_quarantine_name = f"silver_{source_key}_quarantine"
    gold_name = f"gold_{source_key}_counts"

    # Get columns for rule generation (raw table exists because ingestion runs first)
    try:
        raw_cols = spark.table(raw_table).columns
    except Exception:
        raw_cols = []

    rules = _build_rules_for_source(source_key, raw_cols)
    quarantine_expr = "NOT({0})".format(" AND ".join(rules.values()))

    @dp.table(name=bronze_name, comment=f"Bronze layer for source={source_key}")
    def _bronze(raw_table=raw_table, source_key=source_key):
        return (
            spark.readStream.table(raw_table)
            .withColumn("_bronze_ts", F.current_timestamp())
            .withColumn("_source_key", F.lit(source_key))
        )

    @dp.table(
        name=quarantine_stage_name,
        temporary=True,
        partition_cols=["is_quarantined"],
        comment=f"Temp quarantine stage for source={source_key}"
    )
    @dp.expect_all(rules)
    def _silver_quarantine_stage(bronze_name=bronze_name, quarantine_expr=quarantine_expr):
        return (
            spark.readStream.table(bronze_name)
            .withColumn("is_quarantined", expr(quarantine_expr))
        )

    @dp.table(name=silver_name, comment=f"Silver (valid) records for source={source_key}")
    def _silver_valid(stage=quarantine_stage_name):
        return (
            spark.read.table(stage)
            .filter("is_quarantined = false")
            .drop("is_quarantined")
            .withColumn("_silver_ts", F.current_timestamp())
        )

    @dp.table(name=silver_quarantine_name, comment=f"Silver quarantine (invalid) records for source={source_key}")
    def _silver_quarantine(stage=quarantine_stage_name):
        return (
            spark.read.table(stage)
            .filter("is_quarantined = true")
            .drop("is_quarantined")
            .withColumn("_silver_quarantine_ts", F.current_timestamp())
        )

    @dp.materialized_view(name=gold_name, comment=f"Gold counts for source={source_key}")
    def _gold_counts(silver_name=silver_name):
        return (
            spark.read.table(silver_name)
            .groupBy("_source_key")
            .agg(F.count("*").alias("record_count"))
        )
