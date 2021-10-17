from pyspark.sql import SparkSession
import os
from pyspark.sql.types import *
import json


appName = "PySpark Read XML"
master = "local"

if __name__ == '__main__':
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "com.databricks:spark-xml_2.11:0.11.0" pyspark-shell'
    )

    # Create Spark session
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .getOrCreate()

    with open("../resources/books/book_schema.json") as f:
        new_schema = StructType.fromJson(json.load(f))
        print(new_schema.simpleString())

    df = spark.read.format('xml') \
        .options(rowTag='book') \
        .schema(new_schema)\
        .load('../resources/books/books.xml')

    df.show(2)
    # Generate schema
    # ------------------------------
    # schema_json = df.schema.json()
    # print(schema_json)