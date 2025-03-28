import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from source
glue_dynamic_frame_initial = glueContext.create_dynamic_frame.from_catalog(
    database='glue-etl-from-csv-to-parquet',
    table_name='ufo_reports_source_csv'
)

df_spark = glue_dynamic_frame_initial.toDF()
df_spark.show()


def prepare_dataframe(df):
    """Rename the columns, extract year and drop unnecessary columns. Remove NULL records"""
    df_renamed = df.withColumnRenamed("Shape Reported", "shape_reported") \
        .withColumnRenamed("Colors Reported", "color_reported") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Time", "time")

    df_year_added = df_renamed.withColumn(
        "year",
        F.year(F.to_timestamp(F.col("time"), "M/d/yyyy H:mm"))
    ) \
        .drop("time") \
        .drop("city")

    df_final = df_year_added.filter(
        (F.col("shape_reported").isNotNull()) &
        (F.col("color_reported").isNotNull())
    )

    return df_final


def join_dataframes(df):
    """Create color and shape dataframes and join them"""
    shape_grouped = df.groupBy("year", "state", "shape_reported") \
        .agg(F.count("*").alias("shape_occurrence"))

    color_grouped = df.groupBy("year", "state", "color_reported") \
        .agg(F.count("*").alias("color_occurrence"))

    df_joined = shape_grouped.join(
        color_grouped,
        on=["year", "state"],
        how="inner"
    )

    return df_joined


def create_final_dataframe(df):
    """Create final dataframe"""
    shape_window_spec = Window.partitionBy("year", "state") \
        .orderBy(F.col("shape_occurrence").desc())

    color_window_spec = Window.partitionBy("year", "state") \
        .orderBy(F.col("color_occurrence").desc())

    # Selecting top occurrences of shape and color per year and state
    final_df = df.withColumn(
        "shape_rank",
        F.row_number().over(shape_window_spec)
    ) \
        .withColumn(
        "color_rank",
        F.row_number().over(color_window_spec)
    ) \
        .filter(
        (F.col("shape_rank") == 1) &
        (F.col("color_rank") == 1)
    ) \
        .select(
        "year",
        "state",
        "shape_reported",
        "shape_occurrence",
        "color_reported",
        "color_occurrence"
    ) \
        .orderBy(F.col("shape_occurrence").desc())

    return final_df


try:
    # Process the data
    df_prepared = prepare_dataframe(df_spark)
    df_joined = join_dataframes(df_prepared)
    df_final = create_final_dataframe(df_joined)

    # Write the final DataFrame to S3
    df_final.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save("s3://aws-glue-etl-job-spark/ufo_reports_target_parquet")

    # Update the Glue Data Catalog
    spark.sql("DROP TABLE IF EXISTS `glue-etl-from-csv-to-parquet`.ufo_reports_target_parquet")

    spark.sql("""
        CREATE TABLE `glue-etl-from-csv-to-parquet`.ufo_reports_target_parquet
        USING parquet
        LOCATION 's3://aws-glue-etl-job-spark/ufo_reports_target_parquet'
        AS SELECT * FROM df_final
    """)

    print("Data successfully written to S3 and catalog updated")

except Exception as e:
    print(f"Error occurred: {str(e)}")
    raise e

finally:
    job.commit()
