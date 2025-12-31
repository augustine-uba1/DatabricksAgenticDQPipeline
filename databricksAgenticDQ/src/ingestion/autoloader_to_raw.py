import argparse
from pyspark.sql import functions as F


def ensure_schema_exists(fully_qualified_table: str) -> None:
    # Supports catalog.schema.table or schema.table
    parts = fully_qualified_table.split(".")
    if len(parts) >= 2:
        schema = ".".join(parts[:-1])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_path", required=True)
    parser.add_argument("--raw_table", required=True)  # e.g. main.raw.events_json
    parser.add_argument("--format", required=True)     # json/csv/parquet
    parser.add_argument("--schema_location", required=True)
    parser.add_argument("--checkpoint_location", required=True)
    parser.add_argument("--rescued_data_column", default="_rescued_data")
    parser.add_argument("--available_now", default="true")  # "true"/"false"
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
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_input_file", F.input_file_name())
    )

    writer = (
        df.writeStream
        .option("checkpointLocation", args.checkpoint_location)
        .outputMode("append")
    )

    if args.available_now.lower() == "true":
        writer = writer.trigger(availableNow=True)

    q = writer.toTable(args.raw_table)
    q.awaitTermination()


if __name__ == "__main__":
    main()
