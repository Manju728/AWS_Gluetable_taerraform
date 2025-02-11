import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import logging
import boto3

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3_source_path = "s3://my-glue-job-scripts-bucket-ap-south-1/output-parquet/"


database_name = "manju_database"
table_name = "manju_gluetable"


dynamicFrame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_source_path]},
    format="parquet"
)


glueContext.write_dynamic_frame.from_catalog(
    frame=dynamicFrame,
    database=database_name,
    table_name=table_name,
    transformation_ctx="write_to_catalog"
)

print("Data successfully written to AWS Glue Catalog table.")


# def main():
#     spark = SparkSession.builder \
#         .appName("parquetFile") \
#         .getOrCreate()
#     athena_client = boto3.client('athena')

#     data = [("James", "", "Smith", "36636", "M", 3000),
#             ("Michael", "Rose", "", "40288", "M", 4000),
#             ("Robert", "", "Williams", "42114", "M", 4000),
#             ("Maria", "Anne", "Jones", "39192", "F", 4000),
#             ("Jen", "Mary", "Brown", "", "F", -1)]

#     columns = ["fn", "ln", "mn", "id", "gndr", "salary"]
#     df = spark.createDataFrame(data, columns)
#     df.show()

#     logging.info("Schema of the DataFrame:")
#     logging.info(df.printSchema())  # Print schema to logs

#     logging.info("DataFrame content:")
#     df.show(truncate=False)  # Show full content without truncation
#     output_dir = "s3://my-glue-job-scripts-bucket-ap-south-1/output-parquet/"

#     df.write.mode('overwrite').parquet(output_dir)
#     df = spark.read.parquet(output_dir)

#     logging.info("Schema of the DataFrame:")
#     logging.info(df.printSchema())

#     logging.info("DataFrame content:")
#     df.show(truncate=False)

#     s3_source_path = "s3://my-glue-job-scripts-bucket-ap-south-1/output-parquet/"
#     database_name = "manju_database"
#     table_name = "manju_gluetable"
#     df = spark.read.parquet(s3_source_path)
#     glue_df = DynamicFrame.fromDF(df, GlueContext, "glue_df")
#     GlueContext.write_dynamic_frame.from_catalog(
#     frame=glue_df,
#     database=database_name,
#     table_name=table_name
#     )

#     print("Data successfully written to AWS Glue Catalog table.")


# if __name__ == "__main__":
#     main()
