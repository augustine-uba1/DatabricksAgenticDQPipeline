import argparse
import json
import os
from typing import Any, Dict, Optional

from pyspark.sql import functions as F


def _abs_path(relative_or_abs: str) -> str:
    """
    Resolve config path for open() in Databricks Jobs.

    Supports:
      - relative paths (relative to this script)
      - absolute workspace paths (/Workspace/...)
      - file: URIs (file:/Workspace/...)
    """
    if relative_or_abs.startswith("file:"):
        relative_or_abs = relative_or_abs[len("file:") :]

    # Already absolute -> return as-is
    if os.path.isabs(relative_or_abs):
        return relative_or_abs

    # Otherwise resolve relative to this script's folder
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, relative_or_abs))


def _load_config(config_path: str) -> Dict[str, Any]:
    path = _abs_path(config_path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)



def _find_source_args(cfg: Dict[str, Any], source_key: str) -> Dict[str, Any]:
    for s in cfg.get("sources", []):
        if s.get("key") == source_key:
            return s.get("args", {})
    raise ValueError(
        f"Source key '{source_key}' not found in config. "
        f"Available: {[s.get('key') for s in cfg.get('sources', [])]}"
    )


def ensure_schema_exists(fully_qualified_table: str) -> None:
    parts = fully_qualified_table.split(".")
    if len(parts) >= 2:
        schema = ".".join(parts[:-1])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def main() -> None:
    p = argparse.ArgumentParser(description="Databricks Auto Loader -> RAW Delta")

    # Always required (for lineage + config lookup)
    p.add_argument("--source_key", required=True)

    # Optional config lookup
    p.add_argument("--sources_config_path", required=False, default=None)

    # Direct params (optional if using config)
    p.add_argument("--source_path", required=False, default=None)
    p.add_argument("--raw_table", required=False, default=None)
    p.add_argument("--format", required=False, default=None)
    p.add_argument("--schema_location", required=False, default=None)
    p.add_argument("--checkpoint_location", required=False, default=None)
    p.add_argument("--rescued_data_column", required=False, default="_rescued_data")
    p.add_argument("--available_now", required=False, default="true")
    p.add_argument("--processing_time", required=False, default="1 minute")
    p.add_argument("--job_run_id", required=False, default="unknown")

    args = p.parse_args()

    # If config is provided, fill missing values from config for that source_key
    cfg_args: Dict[str, Any] = {}
    if args.sources_config_path:
        cfg = _load_config(args.sources_config_path)
        cfg_args = _find_source_args(cfg, args.source_key)

    def val(cli_val: Optional[str], key: str, default: Optional[str] = None) -> Optional[str]:
        return cli_val if cli_val not in (None, "") else cfg_args.get(key, default)

    source_path = val(args.source_path, "source_path")
    raw_table = val(args.raw_table, "raw_table")
    file_format = val(args.format, "format")
    schema_location = val(args.schema_location, "schema_location")
    checkpoint_location = val(args.checkpoint_location, "checkpoint_location")
    rescued_data_column = val(args.rescued_data_column, "rescued_data_column", "_rescued_data")
    available_now = str(val(args.available_now, "available_now", "true")).lower() == "true"
    processing_time = val(args.processing_time, "processing_time", "1 minute")

    cloudfiles_options: Dict[str, str] = cfg_args.get("cloudfiles_options", {}) or {}

    missing = [k for k, v in {
        "source_path": source_path,
        "raw_table": raw_table,
        "format": file_format,
        "schema_location": schema_location,
        "checkpoint_location": checkpoint_location,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing required args for source_key='{args.source_key}': {missing}")

    ensure_schema_exists(raw_table)

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", rescued_data_column)
    )

    # Apply any per-source extra options from config
    for k, v in cloudfiles_options.items():
        reader = reader.option(k, v)

    df = (
        reader.load(source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_input_file", F.input_file_name())
        .withColumn("_source_key", F.lit(args.source_key))
        .withColumn("_job_run_id", F.lit(args.job_run_id))
    )

    writer = (
        df.writeStream
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
    )

    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=processing_time)

    q = writer.toTable(raw_table)
    q.awaitTermination()


if __name__ == "__main__":
    main()
