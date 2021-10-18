from pyspark.sql import SparkSession
import os
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.sql.types import *
import json
from pyspark.sql.functions import col


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


    def ext_from_xml(xml_column, schema, options={}):
        java_column = _to_java_column(xml_column.cast('string'))
        java_schema = spark._jsparkSession.parseDataType(schema.json())
        scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
        jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
            java_column, java_schema, scala_map)
        return Column(jc)


    def ext_schema_of_xml_df(df, options={}):
        assert len(df.columns) == 1

        scala_options = spark._jvm.PythonUtils.toScalaMap(options)
        java_xml_module = getattr(getattr(
            spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$")
        java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
        return _parse_datatype_json_string(java_schema.json())

    with open("../resources/books/books.xsd", "r") as file:
        xsdasstring = file.read()
    print(str(spark._jvm.com.databricks.spark.xml.util.XSDToSchema.read(xsdasstring)))


    # SparkContext.addFile as the local xsd file will not available in all executors
    spark.sparkContext.addFile("../resources/books/books.xsd")

    with open("../resources/books/book_schema.json") as f:
        new_schema = StructType.fromJson(json.load(f))
        #print(new_schema.simpleString())



    print(new_schema.json())
    df = spark.read.format('csv') \
        .option("header", "true") \
        .option("delimiter", "|") \
        .option("multiLine", True) \
        .option("quote", '~') \
        .option("mode", "PERMISSIVE") \
        .load('../resources/books/dataWithXml.csv')

    # df2 = parsed.select(*parsed.columns[:-1], F.explode(F.col('parsed').getItem('visitor')))
    # df2.show()
    # new_col_names = [s.split(':')[0] for s in
    #                  payloadSchema['visitor'].simpleString().split('<')[-1].strip('>>').split(',')]

    payloadSchema = ext_schema_of_xml_df(df.select("mvr_report"))
    print(payloadSchema.json())
    df.select(ext_from_xml(col("mvr_report"),new_schema)).show()
    # df.select(ext_from_xml(col("mvr_report"), payloadSchema)).show()
    # schema_json = df.schema.json()
    # print(schema_json)
    # df2 = parsed.select(*parsed.columns[:-1], F.explode(F.col('parsed').getItem('visitor')))
    # df2.show()
