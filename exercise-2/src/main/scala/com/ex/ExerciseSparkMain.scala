package com.ex

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object ExerciseSparkMain {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().master("local[*]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
/*
    // Run1:
    val inputPath = "/00_MyDrive/ApacheSpark/00_Projects/exercise/input/run_1"
    val outputPath = "/00_MyDrive/ApacheSpark/00_Projects/exercise/output"
    val batchId = 495
    val lastFullBatchId = None //Optional
    val createFullBatch= true
*/
    // Run2:
    val inputPath = "/00_MyDrive/ApacheSpark/00_Projects/exercise/input/run_1"
    val outputPath = "/00_MyDrive/ApacheSpark/00_Projects/exercise/output"
    val batchId = 597
    val lastFullBatchId = 495 //Optional
    val createFullBatch= false

    // Use: match { case
    // Run1:
    if (createFullBatch==true && lastFullBatchId ==None)
      {

        val compDF = sparkSession.read
          .parquet(inputPath)
          .filter(col("batch_id")<=batchId)
          .withColumn("rnk_attribute_type", row_number()
            .over(Window
              .partitionBy("company_id","attribute_id")
              .orderBy(col("attribute_prob").desc,col("batch_id").desc,col("source_id").asc)))
          .filter(col("rnk_attribute_type")<=1 && col("company_id") === 1000361)
          .withColumn("new_batch_id",lit(batchId))
          .coalesce(1 )
          .select(col("source_id"),
            col("company_id"),
            col("attribute_id"),
            col("attribute_value"),
            col("attribute_prob"),
            col("new_batch_id").alias("batch_id"))

        compDF
          .write
          .partitionBy("batch_id")
          .mode(SaveMode.Overwrite)
          .parquet(outputPath)

      }
    else
      {
        val  incCompDF= sparkSession.read
          .parquet(inputPath)
          .filter(col("batch_id")===batchId)

        val prevFullCompDF = sparkSession.read
          .parquet(outputPath)
          .filter(col("batch_id")===lastFullBatchId)

        val newCompDF=incCompDF.union(prevFullCompDF)
        newCompDF
          .filter(col("company_id") === 1000361)
          .show(false)


        val compDF = newCompDF
          .withColumn("rnk_attribute_type", row_number()
            .over(Window
              .partitionBy("company_id","attribute_id")
              .orderBy(col("attribute_prob").desc,col("batch_id").desc,col("source_id").asc)))
          .filter(col("rnk_attribute_type")<=1 && col("company_id") === 1000361)
          .withColumn("new_batch_id",lit(batchId))
          .coalesce(1 )
          .select(col("source_id"),
            col("company_id"),
            col("attribute_id"),
            col("attribute_value"),
            col("attribute_prob"),
            col("new_batch_id").alias("batch_id"))

        compDF.show()
      }
    /*
+---------+----------+------------+--------------------+------------------+--------+
|source_id|company_id|attribute_id|     attribute_value|    attribute_prob|batch_id|
+---------+----------+------------+--------------------+------------------+--------+
|       41|   1000361|          41|             14000.0|0.9909867742507531|     597|
|       33|   1000361|          57|  Financial Services|0.6205921595700198|     597|
|       41|   1000361|          42|               55000|0.9381583846287386|     597|
|        1|   1000361|          50|                0.95|0.7645849936383715|     597|
|       41|   1000361|          48|              333415|0.9950236664485026|     597|
|       41|   1000361|          81|                null| 0.995514949696081|     597|
|       33|   1000361|          49|Air-Conditioning ...|0.9409503161332292|     597|
|       41|   1000361|          60|linkedin.com/comp...|0.9433040985469994|     597|
+---------+----------+------------+--------------------+------------------+--------+


     */
  }

}
