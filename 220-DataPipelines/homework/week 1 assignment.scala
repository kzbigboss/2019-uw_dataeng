// Databricks notebook source
// DBTITLE 1,Download file from the web
import sys.process._
val command = """curl -o /dbfs/autumn_2019/mmkazzaz/babynames.csv "https://data.cityofnewyork.us/api/views/25th-nujf/rows.csv?accessType=DOWNLOAD""""
Seq("/bin/bash", "-c", command).!!

// COMMAND ----------

// DBTITLE 1,Confirm file availability
// MAGIC %fs ls dbfs:/autumn_2019/mmkazzaz/babynames.csv

// COMMAND ----------

// DBTITLE 1,Create dataframe
val csvFile = "dbfs:/autumn_2019/mmkazzaz/babynames.csv"
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csvFile)
df.printSchema

// COMMAND ----------

// DBTITLE 1,Show sample data
df.show(10)

// COMMAND ----------

// DBTITLE 1,Create summary dataframe
import org.apache.spark.sql.functions._

val dfSummary = df.select($"Child's First Name", $"Count")
  .withColumnRenamed("Child's First Name", "first_name")
  .withColumnRenamed("Count", "count")
  .withColumn("first_name", lower(col("first_name")))
  .groupBy($"first_name")
  .agg(sum($"count") as "sum_name")
//   .sort($"first_name")

dfSummary.show(10)

// COMMAND ----------

// DBTITLE 1,Save summary dataframe as parquet
val fileOutputPath = "dbfs:/autumn_2019/mmkazzaz/babynames_parquet/"
dfSummary.write.parquet(fileOutputPath)

// COMMAND ----------

// DBTITLE 1,Confirm Parquet output
// MAGIC %fs ls dbfs:/autumn_2019/mmkazzaz/babynames_parquet/

// COMMAND ----------

// DBTITLE 1,Find my name in the summary dataframe
val dfMark = dfSummary.filter($"first_name" === "mark")

dfMark.show()
