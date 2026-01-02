import argparse
from pyspark.sql import functions as F



def ensure_schema_exists(fully_qualified_table: str) -> None:
    """
    Supports:
      - catalog.schema.table (Unity Catalog)
      - schema.table
    Creates the schema if missing.
    """
    parts = fully_qualified_table.split(".")
    if len(parts) >= 2:
        schema = ".".join(parts[:-1])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Databricks Auto Loader -> RAW Delta")

    # NEW (for lineage + future fan-out orchestration)
    parser.add_argument("--source_key", required=True, help="Unique key for this source (e.g. 'stock')")

    # Existing
    parser.add_argument("--source_path", required=True, help="Landing path (abfss://... or dbfs:/...)")
    parser.add_argument("--raw_table", required=True, help="Target RAW Delta table (catalog.schema.table)")
    parser.add_argument("--format", required=True, help="File format: parquet/csv/json")
    parser.add_argument("--schema_location", required=True, help="Path for Auto Loader schema metadata")
    parser.add_argument("--checkpoint_location", required=True, help="Path for streaming checkpoint state")
    parser.add_argument("--rescued_data_column", default="_rescued_data", help="Rescued data column name")

    # NEW (trigger control)
    parser.add_argument("--available_now", default="true", help="true|false. true runs once and stops.")
    parser.add_argument(
        "--processing_time",
        default="1 minute",
        help='Used when available_now=false. Example: "30 seconds", "1 minute", "5 minutes".',
    )

    # Optional (helpful for auditing / debugging)
    parser.add_argument("--job_run_id", default="unknown", help="Databricks job run id (optional)")

    args = parser.parse_args()

    ensure_schema_exists(args.raw_table)

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", args.format)
        .option("cloudFiles.schemaLocation", args.schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", args.rescued_data_column)
        .load(args.source_path)
        # Standard ingestion metadata for traceability
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_input_file", F.input_file_name())
        .withColumn("_source_key", F.lit(args.source_key))
        .withColumn("_job_run_id", F.lit(args.job_run_id))
    )

    writer = (
        df.writeStream
        .option("checkpointLocation", args.checkpoint_location)
        .outputMode("append")
    )

    available_now = args.available_now.strip().lower() == "true"

    if available_now:
        # Process everything available and then stop
        writer = writer.trigger(availableNow=True)
    else:
        # Keep running and check for new files on a schedule
        writer = writer.trigger(processingTime=args.processing_time)

    q = writer.toTable(args.raw_table)
    q.awaitTermination()


if __name__ == "__main__":
    main()