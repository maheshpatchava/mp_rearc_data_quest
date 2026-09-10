"""
Delta Live Tables pipeline for BLS and Population data.

Implements Bronze/Silver/Gold medallion architecture.
"""

from typing import Dict, List, Optional

import dlt
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType
)
from pyspark.sql.window import Window


def load_bronze_with_autoloader(
    spark: SparkSession,
    volume_path: str,
    checkpoint_name: str,
    file_format: str,
    path_glob_filter: str,
    subdir: str = "bls",
    separator: Optional[str] = None
) -> DataFrame:
    """
    Generic helper to load files into Bronze with Auto Loader.

    Args:
        spark: SparkSession
        volume_path: Base volume path
        checkpoint_name: Checkpoint subdirectory name
        file_format: 'csv' or 'text'
        path_glob_filter: File pattern (e.g., 'pr.data.*')
        subdir: Subdirectory under volume (default: 'bls')
        separator: Column separator for CSV (default: None)
    """
    stream_reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{volume_path}/_checkpoint/{checkpoint_name}")
        .option("pathGlobFilter", path_glob_filter)
    )

    if file_format == "csv" and separator:
        stream_reader = (
            stream_reader
            .option("sep", separator)
            .option("header", "true")
            .option("inferSchema", "true")
        )

    return (
        stream_reader
        .load(f"{volume_path}/{subdir}/*")
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


def clean_column_names(df: DataFrame) -> DataFrame:
    """Remove trailing spaces from column names (handles TSV header spaces)."""
    for col_name in df.columns:
        if col_name != col_name.strip():
            df = df.withColumnRenamed(col_name, col_name.strip())
    return df


def apply_scd_type1(target_name: str, source_name: str, keys: List[str]) -> None:
    """
    Apply SCD Type 1 pattern (overwrite) to Silver tables.

    Args:
        target_name: Target table name
        source_name: Source view name
        keys: List of key columns for deduplication
    """
    dlt.create_streaming_table(name=target_name)
    dlt.apply_changes(
        target=target_name,
        source=source_name,
        keys=keys,
        sequence_by="ingestion_timestamp",
        stored_as_scd_type=1
    )


def create_bronze_bls_data_table(spark: SparkSession, volume_path: str):
    """Bronze: Raw BLS productivity data (pr.data.* files only)."""

    @dlt.table(
        name="bronze_bls_data",
        comment="Raw BLS productivity data - tab-separated",
        table_properties={
            "quality": "bronze",
            "delta.columnMapping.mode": "name"
        }
    )
    def bronze_bls_data():
        return load_bronze_with_autoloader(
            spark, volume_path, "bls_data", "csv", "pr.data.*", separator="\t"
        )


def create_bronze_bls_series_table(spark: SparkSession, volume_path: str):
    """Bronze: Raw BLS series metadata (pr.series file)."""

    @dlt.table(
        name="bronze_bls_series",
        comment="Raw BLS series metadata - tab-separated",
        table_properties={
            "quality": "bronze",
            "delta.columnMapping.mode": "name"
        }
    )
    def bronze_bls_series():
        return load_bronze_with_autoloader(
            spark, volume_path, "bls_series", "csv", "pr.series", separator="\t"
        )


def create_bronze_bls_lookups(spark: SparkSession, volume_path: str):
    """Bronze: BLS lookup tables for human-readable labels."""

    @dlt.table(
        name="bronze_bls_measure",
        comment="BLS measure code lookup",
        table_properties={
            "quality": "bronze",
            "delta.columnMapping.mode": "name"
        }
    )
    def bronze_bls_measure():
        return load_bronze_with_autoloader(
            spark, volume_path, "bls_measure", "csv", "pr.measure", separator="\t"
        )

    @dlt.table(
        name="bronze_bls_sector",
        comment="BLS sector code lookup",
        table_properties={
            "quality": "bronze",
            "delta.columnMapping.mode": "name"
        }
    )
    def bronze_bls_sector():
        return load_bronze_with_autoloader(
            spark, volume_path, "bls_sector", "csv", "pr.sector", separator="\t"
        )

    @dlt.table(
        name="bronze_bls_class",
        comment="BLS class code lookup",
        table_properties={
            "quality": "bronze",
            "delta.columnMapping.mode": "name"
        }
    )
    def bronze_bls_class():
        return load_bronze_with_autoloader(
            spark, volume_path, "bls_class", "csv", "pr.class", separator="\t"
        )


def create_bronze_population_table(spark: SparkSession, volume_path: str):
    """Bronze: Raw population data as single-line JSON."""

    @dlt.table(
        name="bronze_population",
        comment="Raw population API response - compact JSON (one line)",
        table_properties={"quality": "bronze"}
    )
    def bronze_population():
        return load_bronze_with_autoloader(
            spark, volume_path, "population", "text", "*.json", subdir="population"
        )


def create_silver_bls_data_table():
    """Silver: Cleaned BLS data with deduplication using SCD Type 1."""

    @dlt.view(name="silver_bls_data_cleaned")
    @dlt.expect_or_drop("valid_year", "year IS NOT NULL")
    @dlt.expect_or_drop("valid_value", "value IS NOT NULL")
    @dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND series_id != ''")
    def silver_bls_data_cleaned():
        bronze_df = clean_column_names(dlt.read_stream("bronze_bls_data"))

        return bronze_df.select(
            F.trim(F.col("series_id")).alias("series_id"),
            F.col("year").cast(IntegerType()).alias("year"),
            F.trim(F.col("period")).alias("period"),
            F.col("value").cast(DoubleType()).alias("value"),
            F.trim(F.col("footnote_codes")).alias("footnote_codes"),
            F.col("source_file"),
            F.col("ingestion_timestamp")
        )

    apply_scd_type1("silver_bls_data", "silver_bls_data_cleaned",
                    ["series_id", "year", "period"])


def create_silver_bls_series_table():
    """Silver: Cleaned BLS series metadata with component codes using SCD Type 1."""

    @dlt.view(name="silver_bls_series_cleaned")
    @dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND series_id != ''")
    def silver_bls_series_cleaned():
        bronze_df = clean_column_names(dlt.read_stream("bronze_bls_series"))

        return bronze_df.select(
            F.trim(F.col("series_id")).alias("series_id"),
            F.trim(F.col("sector_code")).alias("sector_code"),
            F.trim(F.col("class_code")).alias("class_code"),
            F.trim(F.col("measure_code")).alias("measure_code"),
            F.trim(F.col("duration_code")).alias("duration_code"),
            F.trim(F.col("seasonal")).alias("seasonal"),
            F.col("source_file"),
            F.col("ingestion_timestamp")
        )

    apply_scd_type1("silver_bls_series", "silver_bls_series_cleaned", ["series_id"])


def create_silver_bls_lookups():
    """Silver: Cleaned BLS lookup tables using SCD Type 1."""

    @dlt.view(name="silver_bls_measure_cleaned")
    def silver_bls_measure_cleaned():
        bronze_df = clean_column_names(dlt.read_stream("bronze_bls_measure"))

        return bronze_df.select(
            F.trim(F.col("measure_code")).alias("measure_code"),
            F.trim(F.col("measure_text")).alias("measure_text"),
            F.col("ingestion_timestamp")
        )

    apply_scd_type1("silver_bls_measure", "silver_bls_measure_cleaned", ["measure_code"])

    @dlt.view(name="silver_bls_sector_cleaned")
    def silver_bls_sector_cleaned():
        bronze_df = clean_column_names(dlt.read_stream("bronze_bls_sector"))

        return bronze_df.select(
            F.trim(F.col("sector_code")).alias("sector_code"),
            F.trim(F.col("sector_name")).alias("sector_name"),
            F.col("ingestion_timestamp")
        )

    apply_scd_type1("silver_bls_sector", "silver_bls_sector_cleaned", ["sector_code"])

    @dlt.view(name="silver_bls_class_cleaned")
    def silver_bls_class_cleaned():
        bronze_df = clean_column_names(dlt.read_stream("bronze_bls_class"))

        return bronze_df.select(
            F.trim(F.col("class_code")).alias("class_code"),
            F.trim(F.col("class_text")).alias("class_text"),
            F.col("ingestion_timestamp")
        )

    apply_scd_type1("silver_bls_class", "silver_bls_class_cleaned", ["class_code"])


def create_silver_population_table():
    """Silver: Parsed population data using SCD Type 1."""

    # Schema for the full API response (includes 'data' array)
    api_response_schema = StructType([
        StructField("data", ArrayType(
            StructType([
                StructField("Nation ID", StringType()),
                StructField("Nation", StringType()),
                StructField("Year", LongType()),
                StructField("Population", DoubleType())
            ])
        ))
    ])

    @dlt.view(name="silver_population_cleaned")
    @dlt.expect_or_drop("valid_year", "Year IS NOT NULL")
    @dlt.expect_or_drop("valid_population", "Population IS NOT NULL")
    def silver_population_cleaned():
        bronze_df = dlt.read_stream("bronze_population")

        # Parse single-line JSON from 'value' column
        parsed_df = bronze_df.withColumn(
            "parsed", F.from_json(F.col("value"), api_response_schema)
        )

        # Explode the 'data' array from API response
        exploded_df = parsed_df.select(
            F.explode("parsed.data").alias("record"),
            "source_file",
            "ingestion_timestamp"
        )

        # Extract fields from record
        return exploded_df.select(
            F.col("record.`Nation ID`").alias("Nation_ID"),
            F.col("record.Nation"),
            F.col("record.Year").cast(IntegerType()).alias("Year"),
            F.col("record.Population").cast(DoubleType()).alias("Population"),
            F.col("source_file"),
            F.col("ingestion_timestamp")
        )

    apply_scd_type1("silver_population", "silver_population_cleaned", ["Nation_ID", "Year"])


def create_gold_population_stats(spark: SparkSession, use_sql: bool = False):
    """Gold: Population statistics 2013-2018 (PySpark or SQL implementation)."""

    if use_sql:
        @dlt.table(
            name="gold_population_stats",
            comment="Mean and std dev of US population 2013-2018 - Spark SQL",
            table_properties={"quality": "gold"}
        )
        def gold_population_stats():
            return spark.sql("""
                SELECT
                    AVG(Population) AS mean_population,
                    STDDEV(Population) AS stddev_population,
                    MIN(Year) AS start_year,
                    MAX(Year) AS end_year,
                    COUNT(*) AS num_years
                FROM LIVE.silver_population
                WHERE Year BETWEEN 2013 AND 2018
            """)
    else:
        @dlt.table(
            name="gold_population_stats",
            comment="Mean and std dev of US population 2013-2018 - PySpark",
            table_properties={"quality": "gold"}
        )
        def gold_population_stats():
            return (
                dlt.read("silver_population")
                .filter((F.col("Year") >= 2013) & (F.col("Year") <= 2018))
                .agg(
                    F.mean("Population").alias("mean_population"),
                    F.stddev("Population").alias("stddev_population"),
                    F.min("Year").alias("start_year"),
                    F.max("Year").alias("end_year"),
                    F.count("*").alias("num_years")
                )
            )


def create_gold_best_year_by_series(spark: SparkSession, use_sql: bool = False):
    """Gold: Best year for each BLS series with human-readable labels."""

    if use_sql:
        @dlt.table(
            name="gold_best_year_by_series",
            comment="Best year (max total value) for each series_id with descriptions - Spark SQL",
            table_properties={"quality": "gold"}
        )
        def gold_best_year_by_series():
            return spark.sql("""
                WITH yearly_sums AS (
                    SELECT
                        d.series_id,
                        d.year,
                        SUM(d.value) AS total_value
                    FROM LIVE.silver_bls_data d
                    GROUP BY d.series_id, d.year
                ),
                ranked_years AS (
                    SELECT
                        series_id,
                        year,
                        total_value,
                        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY total_value DESC) AS rank
                    FROM yearly_sums
                )
                SELECT
                    r.series_id,
                    CONCAT_WS(' - ',
                        COALESCE(sec.sector_name, s.sector_code),
                        COALESCE(cls.class_text, s.class_code),
                        COALESCE(msr.measure_text, s.measure_code)
                    ) AS series_description,
                    r.year AS best_year,
                    r.total_value
                FROM ranked_years r
                LEFT JOIN LIVE.silver_bls_series s ON r.series_id = s.series_id
                LEFT JOIN LIVE.silver_bls_sector sec ON s.sector_code = sec.sector_code
                LEFT JOIN LIVE.silver_bls_class cls ON s.class_code = cls.class_code
                LEFT JOIN LIVE.silver_bls_measure msr ON s.measure_code = msr.measure_code
                WHERE r.rank = 1
            """)
    else:
        @dlt.table(
            name="gold_best_year_by_series",
            comment="Best year (max total value) for each series_id with descriptions - PySpark",
            table_properties={"quality": "gold"}
        )
        def gold_best_year_by_series():
            data_df = dlt.read("silver_bls_data")
            series_df = dlt.read("silver_bls_series")
            sector_df = dlt.read("silver_bls_sector")
            class_df = dlt.read("silver_bls_class")
            measure_df = dlt.read("silver_bls_measure")

            yearly_sums = data_df.groupBy("series_id", "year") \
                                 .agg(F.sum("value").alias("total_value"))

            window_spec = Window.partitionBy("series_id") \
                               .orderBy(F.desc("total_value"))

            best_years = (
                yearly_sums
                .withColumn("rank", F.row_number().over(window_spec))
                .filter(F.col("rank") == 1)
                .drop("rank")
            )

            # Join with series metadata and lookups
            result = (
                best_years
                .join(series_df, "series_id", "left")
                .join(sector_df, "sector_code", "left")
                .join(class_df, "class_code", "left")
                .join(measure_df, "measure_code", "left")
            )

            # Construct human-readable description
            return result.select(
                "series_id",
                F.concat_ws(" - ",
                    F.coalesce(F.col("sector_name"), F.col("sector_code")),
                    F.coalesce(F.col("class_text"), F.col("class_code")),
                    F.coalesce(F.col("measure_text"), F.col("measure_code"))
                ).alias("series_description"),
                F.col("year").alias("best_year"),
                "total_value"
            )


def create_gold_bls_population_join(spark: SparkSession, use_sql: bool = False):
    """Gold: BLS data joined with population by year (PySpark or SQL implementation)."""

    if use_sql:
        @dlt.table(
            name="gold_bls_population_join",
            comment="BLS data enriched with population data - Spark SQL",
            table_properties={"quality": "gold"}
        )
        def gold_bls_population_join():
            return spark.sql("""
                SELECT
                    d.series_id,
                    d.year,
                    d.period,
                    d.value,
                    p.Population
                FROM LIVE.silver_bls_data d
                LEFT JOIN LIVE.silver_population p ON d.year = p.Year
            """)
    else:
        @dlt.table(
            name="gold_bls_population_join",
            comment="BLS data enriched with population data - PySpark",
            table_properties={"quality": "gold"}
        )
        def gold_bls_population_join():
            data_df = dlt.read("silver_bls_data")
            pop_df = dlt.read("silver_population")

            return (
                data_df
                .join(pop_df, data_df.year == pop_df.Year, "left")
                .select(
                    "series_id",
                    data_df.year.alias("year"),
                    "period",
                    "value",
                    "Population"
                )
            )


def main(params: Dict):
    """Initialize DLT pipeline with all Bronze/Silver/Gold tables.

    Args:
        params: Configuration including:
            - catalog_name, schema_name, volume_name
            - use_sql: True to use Spark SQL, False for PySpark (default: False)
    """
    spark = SparkSession.builder.getOrCreate()

    catalog_name = params.get('catalog_name', 'main')
    schema_name = params.get('schema_name', 'rearc_data_quest')
    volume_name = params.get('volume_name', 'raw_data')
    use_sql = params.get('use_sql', 'false').lower() == 'true'

    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

    # Bronze layer
    create_bronze_bls_data_table(spark, volume_path)
    create_bronze_bls_series_table(spark, volume_path)
    create_bronze_bls_lookups(spark, volume_path)
    create_bronze_population_table(spark, volume_path)

    # Silver layer
    create_silver_bls_data_table()
    create_silver_bls_series_table()
    create_silver_bls_lookups()
    create_silver_population_table()

    # Gold layer (PySpark primary, SQL alternative via use_sql param)
    create_gold_population_stats(spark, use_sql)
    create_gold_best_year_by_series(spark, use_sql)
    create_gold_bls_population_join(spark, use_sql)
