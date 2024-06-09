# Databricks notebook source
DIRECTORY = "/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/broadcast_logs/BroadcastLogs_2018_Q3_M8_sample.CSV"
logs = (
    spark.read.csv(
        sep="|",
        header=True,
        inferSchema=True,
        timestampFormat="yyyy-MM-dd",
        path=DIRECTORY,
    )
)

# COMMAND ----------

logs.printSchema()

# COMMAND ----------

logs.select("BroadcastLogID","LogServiceID","LogDate").show(5, False)

# COMMAND ----------

logs = logs.drop("BroadcastLogID", "SequenceNO")
print("BroadcastLogID" in logs.columns)
print("SequenceNO" in logs.columns)

# COMMAND ----------

import pyspark.sql.functions as F
logs.select(F.col("Duration")).show(5)


# COMMAND ----------

logs.dtypes

# COMMAND ----------

logs.select(
    F.col("DURATION"),
    F.col("Duration").substr(1,2).cast("int").alias("dur_hours"),
    F.col("Duration").substr(4,2).cast("int").alias("dur_minutes"),
    F.col("Duration").substr(7,2).cast("int").alias("dur_seconds"),
)


# COMMAND ----------

logs = logs.withColumnRenamed("Duration in seconds", "duration in seconds")
logs.printSchema()

# COMMAND ----------

logs.toDF(*[x.lower() for x in logs.columns]).printSchema()

# COMMAND ----------

logs.select(sorted(logs.columns)).printSchema()

# COMMAND ----------

display(logs.describe())

# COMMAND ----------

for i in logs.columns:
    logs.describe(i).show()

# COMMAND ----------

display(logs.summary())

# COMMAND ----------

for i in logs.columns:
    logs.select(i).summary().show()

# COMMAND ----------

logs.select(
    F.col("DURATION"),
    (F.col("Duration").substr(1,2).cast("int")*60*60
    + F.col("Duration").substr(4,2).cast("int")*60
    + F.col("Duration").substr(7,2).cast("int")).alias("Duration in seconds"),
).distinct().show()

# COMMAND ----------

logs = logs.withColumn(
    "Duration in seconds",
    (
    F.col("Duration").substr(1,2).cast("int")*60*60
    + F.col("Duration").substr(4,2).cast("int")*60
    + F.col("Duration").substr(7,2).cast("int"))
    )

# COMMAND ----------

logs.columns

# COMMAND ----------

import numpy as np
column_split = np.array_split(np.array(logs.columns), len(logs.columns) // 3)
print(column_split)


# COMMAND ----------

for x in column_split:
    logs.select(*x).show(5, False)

# COMMAND ----------

type(column_split)

# COMMAND ----------




DIRECTORY = "/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/broadcast_logs/BroadcastLogs_2018_Q3_M8_sample.CSV"
logs = (
    spark.read.csv(
        sep="|",
        header=True,
        inferSchema=True,
        timestampFormat="yyyy-MM-dd",
        path=DIRECTORY,
    )
    .drop("BroadcastLogID", "SequenceNO")
    .withColumn(
        "duration_seconds",
        (
            F.col("Duration").substr(1, 2).cast("int") * 60 * 60
            + F.col("Duration").substr(4, 2).cast("int") * 60
            + F.col("Duration").substr(7, 2).cast("int")
        ),
    )
)

# COMMAND ----------

display(logs)

# COMMAND ----------

logs.printSchema()

# COMMAND ----------

logs.selct("")
