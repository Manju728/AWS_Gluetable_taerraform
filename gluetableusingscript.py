from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test-spark-session").getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.canned.acl", "BucketOwnerFullControl")
data = [("James", "", "Smith", "36636", "M", 3000),
        ("Michael", "Rose", "", "40288", "M", 4000),
        ("Robert", "", "Williams", "42114", "M", 4000),
        ("Maria", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)]

columns = ["fn", "ln", "mn", "id", "gndr", "salary"]
df = spark.createDataFrame(data, columns)
df.show()
spark.sql("show databases").show()
db_name = "manju_database"
table_name = "test_table"
df.write.options(path=f"s3://my-glue-job-scripts-bucket-ap-south-1/test/test").format("parquet").mode("append").saveAsTable(f"{db_name}.{table_name}")