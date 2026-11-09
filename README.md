# iteso-pdm

Class projects for Big Data (ITESO, Autumn 2026).

## batch-processing-project

Batch-processing pipeline over the Overture Maps "segment" dataset
(theme=transportation, release 2026-10-21.0), built with Apache Spark and
deployed on AWS EMR: data cleaning, column derivation, filtering/sorting,
aggregation, and persistence as Parquet partitioned by road class.

- `project.py` — driver with the `main()` block (orchestration only)
- `spark_utils_emr.py` — all transformations/actions as methods of the `SparkUtils` class
