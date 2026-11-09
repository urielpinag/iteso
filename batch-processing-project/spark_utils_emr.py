from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, StructField, ArrayType
from pyspark.sql import functions as F
from pyspark.sql.functions import count, when, isnull

class SparkUtils:

    def __init__(self, app_name, master_url=None, spark_jars=None, spark_packages=None):
        spark_builder = (
            SparkSession.builder
            .appName(app_name))
        
        if master_url is not None:
            spark_builder = spark_builder.master(master_url)

        if spark_jars is not None:
            spark_builder = spark_builder.config("spark.jars", spark_jars)

        if spark_packages is not None:
            spark_builder = spark_builder.config("spark.jars.packages", spark_packages)

        self._spark = spark_builder.getOrCreate()

    @property
    def spark(self):
        return self._spark

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        """
        Generates a list of StructField objects from a list of tuples.

        Args:
            column_info (list of tuples): Each tuple contains (column_name, data_type_string).

        Returns:
            list: A list of StructField objects.
        """
        # Mapping from string type names to PySpark data types
        type_mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType(),
            "array_int": ArrayType(IntegerType()),
            "array_string": ArrayType(StringType()),
            "struct": StructType()
        }

        struct_fields = []
        for column_info in columns_info:
            col_name  = column_info[0]
            col_type  = column_info[1]

            if col_type == "struct":
                # Expect a third element with the sub-field definitions
                if len(column_info) < 3 or not isinstance(column_info[2], list):
                    raise ValueError(
                        f"Column '{col_name}' is declared as 'struct' but "
                        f"no sub-fields list was provided as the third element."
                    )
                # Recurse to build the nested StructType
                nested_schema = SparkUtils.generate_schema(column_info[2])
                struct_field  = StructField(col_name, nested_schema, True)

            elif col_type not in type_mapping:
                raise ValueError(f"Unsupported data type: '{col_type}' for column '{col_name}'")

            else:
                # Create a StructField for the column
                struct_field = StructField(column_info[0], type_mapping[column_info[1]], True)
            
            struct_fields.append(struct_field)

        return StructType(struct_fields) 

    @staticmethod
    def count_nulls(df):
        return df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])

    # ------------------------------------------------------------------
    # Project pipeline: derivation -> cleaning -> filter/sort ->
    # aggregation -> persistence. All Spark logic lives in this class.
    # Derivation runs first on purpose: flattening and selecting the flat
    # columns before cleaning lets Spark prune the heavy columns (geometry,
    # nested payloads) during the scans and keeps the dedup shuffle small.
    # ------------------------------------------------------------------

    # Flat columns kept after derivation; used in the report and in the
    # Parquet output (nested payloads and the binary geometry are dropped)
    SEGMENT_COLUMNS = [
        "id", "name", "subtype", "class", "subclass",
        "num_connectors", "num_sources", "num_routes",
        "speed_limit_kph", "lon_center", "lat_center", "version",
    ]

    @staticmethod
    def derive_columns(df):
        """
        Column derivation: flattens nested attributes into flat columns
        (built-in expressions only, no UDFs) and selects those columns,
        so Spark prunes the heavy nested ones during the scans.
        """
        first_limit = F.element_at(F.col("speed_limits"), 1)  # first speed rule (or null)
        speed_value = first_limit["max_speed"]["value"]
        speed_unit = first_limit["max_speed"]["unit"]
        return (
            df.select(
                "id",
                F.col("names.primary").alias("name"),
                "subtype", "class", "subclass",
                # null array = no connectors/sources/routes (size() alone would give null)
                F.when(F.col("connectors").isNotNull(), F.size("connectors"))
                 .otherwise(F.lit(0)).alias("num_connectors"),
                F.when(F.col("sources").isNotNull(), F.size("sources"))
                 .otherwise(F.lit(0)).alias("num_sources"),
                F.when(F.col("routes").isNotNull(), F.size("routes"))
                 .otherwise(F.lit(0)).alias("num_routes"),
                # First speed limit normalized to KPH (some records are in MPH)
                F.when(speed_unit == "MPH", speed_value * F.lit(1.60934))
                 .otherwise(speed_value).alias("speed_limit_kph"),
                ((F.col("bbox.xmin") + F.col("bbox.xmax")) / 2).alias("lon_center"),
                ((F.col("bbox.ymin") + F.col("bbox.ymax")) / 2).alias("lat_center"),
                "version",
            )
        )

    @staticmethod
    def clean_metrics(df):
        """Row count + integrity metrics for the cleaning report (single pass)."""
        return df.agg(
            count("*").alias("rows"),
            count(when(isnull("id"), "id")).alias("null_id"),
            count(when(F.col("num_connectors") == 0, F.lit(1))).alias("no_connectors"),
            count(when(isnull("name"), "name")).alias("null_name"),
            count(when(isnull("speed_limit_kph"), "speed_limit_kph")).alias("null_speed_limit"),
        ).first().asDict()

    @staticmethod
    def clean_segments(df):
        """
        Data cleaning: removes records with null id, duplicated ids and
        segments without connectors. The result is cached, because this
        DataFrame feeds every stage after the cleaning.

        Returns (cleaned_df, report_lines).
        """
        before = SparkUtils.clean_metrics(df)

        cleaned = (
            df.filter(F.col("id").isNotNull())      # malformed records
              .dropDuplicates(["id"])               # duplicated records
              .filter(F.col("num_connectors") > 0)  # segments without connectors
              .cache()
        )
        after = SparkUtils.clean_metrics(cleaned)

        keys = ["null_id", "no_connectors", "null_name", "null_speed_limit"]
        labels = ["null ids", "no connectors", "null names", "null speed limit"]
        lines = [
            "=== DATA CLEANING ===",
            f"Rows: {before['rows']:,} -> {after['rows']:,} "
            f"(removed {before['rows'] - after['rows']:,})",
            "Metric (before -> after):",
        ]
        lines += [f"  {label}: {before[k]:,} -> {after[k]:,}"
                  for k, label in zip(keys, labels)]
        return cleaned, lines

    @staticmethod
    def filter_sort_segments(df, top_n=10):
        """
        Filtering and sorting: keeps only named road segments and builds a
        top-N preview ordered by connectivity and speed limit.

        Returns (filtered_df, top_df).
        """
        filtered = df.filter(
            (F.col("subtype") == "road") & F.col("name").isNotNull()
        )
        top = (filtered.orderBy(F.desc("num_connectors"), F.desc("speed_limit_kph"), F.desc("version"))
                       .limit(top_n))
        return filtered, top

    @staticmethod
    def aggregate_by_class(df):
        """
        Aggregation: summary metrics per road class.
        """
        return (df.groupBy("class")
                  .agg(count("*").alias("segments"),
                       F.round(F.avg("num_connectors"), 2).alias("avg_connectors"),
                       F.round(F.avg("speed_limit_kph"), 2).alias("avg_speed_kph"))
                  .orderBy(F.desc("segments")))

    @staticmethod
    def aggregations_as_text(agg_df, max_classes=30):
        """Collects the (small) per-class aggregation and formats it as text."""
        lines = [f"{'class':<20}{'segments':>12}{'avg_connectors':>16}{'avg_speed_kph':>15}"]
        for row in agg_df.take(max_classes):
            avg_conn = row["avg_connectors"]
            avg_speed = row["avg_speed_kph"]
            lines.append(
                f"{row['class']:<20}{row['segments']:>12,}"
                f"{avg_conn if avg_conn is not None else '-':>16}"
                f"{avg_speed if avg_speed is not None else '-':>15}"
            )
        return "\n".join(lines)

    @staticmethod
    def table_as_text(df, cols=None, rows=10, truncate=40):
        """Returns a compact (non-vertical) table preview as a string."""
        preview = df if cols is None else df.select(*cols)
        return preview._jdf.showString(rows, truncate, False)

    def write_partitioned(self, df, path):
        """
        Data persistence: writes the DataFrame as Parquet partitioned by
        road class (overwrite mode, so the step is re-runnable).
        """
        (df.write
           .mode("overwrite")
           .partitionBy("class")
           .parquet(path))

    def validate_readback(self, path, rows=5):
        """
        Optional validation: reads back the persisted Parquet and returns a
        text sample to confirm the partitioning and the content.
        """
        df = self.spark.read.parquet(path)
        cols = [c for c in SparkUtils.SEGMENT_COLUMNS if c in df.columns]
        partition_dirs = sorted({f.rsplit("/", 1)[0] for f in df.inputFiles()})
        lines = [
            "=== VALIDATION (read back from the persisted Parquet) ===",
            f"Files read back: {len(df.inputFiles())}",
            "Partition folders:",
        ]
        lines += [f"  {d}" for d in partition_dirs]
        lines.append(SparkUtils.sample_as_text(df.select(*cols), rows))
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # Debugging helpers (useful on EMR, where stdout is buried in logs)
    # ------------------------------------------------------------------

    @staticmethod
    def schema_as_text(df) -> str:
        """Returns the same tree that df.printSchema() prints, as a string."""
        return df._jdf.schema().treeString()

    @staticmethod
    def sample_as_text(df, rows=20, truncate=80, vertical=True) -> str:
        """Returns the same table that df.show() prints, as a string."""
        return df._jdf.showString(rows, truncate, vertical)

    @staticmethod
    def sample_as_json(df, rows=20) -> list:
        """Returns the first rows as JSON strings (keeps nested structs readable)."""
        return df.limit(rows).toJSON().collect()

    def debug_report(self, df, title="", rows=20, count_rows=False) -> str:
        """Builds a text report with input files, schema and a sample of records."""
        sample = df.limit(rows)
        files = df.inputFiles()

        lines = [f"Debug report - {datetime.now(timezone.utc).isoformat()}", title, ""]

        lines.append(f"Files read: {len(files)}")
        for f in files[:10]:
            lines.append(f"  {f}")
        if len(files) > 10:
            lines.append(f"  ... and {len(files) - 10} more")
        lines.append(f"Columns: {len(df.columns)}")
        if count_rows:
            lines.append(f"Row count: {df.count():,}")  # full scan
        lines.append("")

        lines.append("=== SCHEMA ===")
        lines.append(SparkUtils.schema_as_text(df))

        lines.append(f"=== SAMPLE ({rows} rows, vertical) ===")
        lines.append(SparkUtils.sample_as_text(sample, rows))

        lines.append(f"=== SAMPLE ({rows} rows, JSON) ===")
        lines.extend(SparkUtils.sample_as_json(sample, rows))

        return "\n".join(lines) + "\n"

    def write_text(self, path, text):
        """
        Writes a single text file using Hadoop's FileSystem API.
        Works with s3://, hdfs:// and file:// paths (overwrites if it exists).
        """
        jvm = self._spark._jvm
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = hadoop_path.getFileSystem(self._spark._jsc.hadoopConfiguration())
        stream = fs.create(hadoop_path, True)
        try:
            stream.write(bytearray(text.encode("utf-8")))
        finally:
            stream.close()