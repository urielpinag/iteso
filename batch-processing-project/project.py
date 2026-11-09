"""
project.py
Batch-processing pipeline over the Overture Maps "segment" dataset
(theme=transportation): column derivation, data cleaning, filtering and
sorting, aggregation, and persistence as Parquet partitioned by road class.

All Spark logic is implemented as methods of SparkUtils (spark_utils_emr.py);
this driver only orchestrates the pipeline.

Usage (EMR step / spark-submit):
    spark-submit --py-files s3://<bucket>/src/spark_utils_emr.py \
        s3://<bucket>/src/project.py \
        --input  s3://<bucket>/data/project/ \
        --output s3://<bucket>/output/transformations_report.txt \
        --parquet-output s3://<bucket>/output/segments_by_class/ \
        --validate
"""
import argparse
from datetime import datetime, timezone

from spark_utils_emr import SparkUtils


def parse_args():
    parser = argparse.ArgumentParser(description="Overture segments batch pipeline")
    parser.add_argument("--input", required=True, help="Parquet file or folder (s3://... or local path)")
    parser.add_argument("--output", required=True, help="Text report to write (s3://... or local path)")
    parser.add_argument("--parquet-output", required=True, help="Folder for the partitioned Parquet output (s3://... or local path)")
    parser.add_argument("--rows", type=int, default=5, help="Sample rows for the report")
    parser.add_argument("--top", type=int, default=10, help="Top segments shown in the report")
    parser.add_argument("--validate", action="store_true", help="Read back the persisted data to validate it")
    return parser.parse_args()


def main():
    args = parse_args()
    su = SparkUtils("batch-processing-project")

    try:
        # Input: the schema is embedded in the Parquet files, no need to set it
        df = su.spark.read.parquet(args.input)
        report = [
            f"Pipeline report - {datetime.now(timezone.utc).isoformat()}",
            f"Input: {args.input}",
            f"Files read: {len(df.inputFiles())}",
            "",
        ]

        # 1. Column derivation (flattens the nested attributes; the pipeline
        #    works on a flat projection, so Spark prunes the heavy columns)
        derived = SparkUtils.derive_columns(df)
        report.append("=== COLUMN DERIVATION ===")
        report.append(SparkUtils.schema_as_text(derived))
        report.append(SparkUtils.sample_as_text(derived, args.rows))
        report.append("")

        # 2. Data cleaning (the cleaned DataFrame is cached, since the rest
        #    of the pipeline reuses it instead of rescanning the input)
        cleaned, cleaning_lines = SparkUtils.clean_segments(derived)
        report += cleaning_lines + [""]

        # 3. Filtering and sorting
        filtered, top_df = SparkUtils.filter_sort_segments(cleaned, top_n=args.top)
        report.append("=== FILTERING & SORTING ===")
        report.append(f"Rows kept by the filter (subtype='road' with a name): {filtered.count():,}")
        report.append(f"Top {args.top} segments by connectivity and speed limit:")
        report.append(SparkUtils.table_as_text(top_df, SparkUtils.SEGMENT_COLUMNS, rows=args.top))
        report.append("")

        # 4. Aggregation
        agg_df = SparkUtils.aggregate_by_class(filtered)
        report.append("=== AGGREGATION (metrics per road class) ===")
        report.append(SparkUtils.aggregations_as_text(agg_df))
        report.append("")

        su.write_text(args.output, "\n".join(report) + "\n")

        # 5. Persistence (Parquet partitioned by road class)
        print(f"Persisting partitioned output to {args.parquet_output}")
        su.write_partitioned(filtered, args.parquet_output)

        # 6. Optional validation of the persisted data
        if args.validate:
            report.append(su.validate_readback(args.parquet_output, rows=args.rows))
            su.write_text(args.output, "\n".join(report) + "\n")

        print(f"Report written to {args.output}")
    except Exception as e:
        # Still leave a report behind, so a failed step can be diagnosed from S3
        su.write_text(args.output, f"Input: {args.input}\n\n=== ERROR ===\n{type(e).__name__}: {e}\n")
        su.spark.stop()
        raise

    su.spark.stop()


if __name__ == "__main__":
    main()