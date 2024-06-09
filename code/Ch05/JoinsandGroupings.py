# Databricks notebook source
import pyspark.sql.functions as F
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

logs.printSchema()

# COMMAND ----------

log_identifier = (
    spark.read.csv(
        sep="|",
        header=True,
        inferSchema=True,
        timestampFormat="yyyy-MM-dd",
        path="/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/broadcast_logs/ReferenceTables/LogIdentifier.csv",
    )
)
display(log_identifier)

# COMMAND ----------

log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)
log_identifier.count()

# COMMAND ----------

logs_and_channels = logs.join(
    log_identifier,
    on="LogServiceID",
    how="inner"
)
logs_and_channels.printSchema()

# COMMAND ----------

logs_and_channels.count()

# COMMAND ----------

logs_and_channels_verbose = logs.join(
    log_identifier, logs["LogServiceID"] == log_identifier["LogServiceID"]
)
logs_and_channels_verbose.printSchema()

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
try:
    logs_and_channels_verbose.select("LogServiceID") 
except AnalysisException as err :
    print(err)

# COMMAND ----------

log_test = logs_and_channels_verbose.select("LogServiceID")
display(log_test)

# COMMAND ----------

logs_and_channels_verbose = logs.join(
    log_identifier, logs["LogServiceID"] == log_identifier["LogServiceID"])
logs_and_channels_verbose.printSchema()

# COMMAND ----------

logs_and_channels_verbose = logs_and_channels_verbose.drop(log_identifier["LogServiceID"])
logs_and_channels_verbose.printSchema()

# COMMAND ----------

logs_and_channels_verbose = logs.alias("left").join(
    log_identifier.alias("right"), logs["LogServiceID"] == log_identifier["LogServiceID"])
logs_and_channels_verbose.printSchema()

# COMMAND ----------

logs_and_channels_verbose = logs_and_channels_verbose.drop(F.col("right.LogServiceID"))
logs_and_channels_verbose.printSchema()

# COMMAND ----------

cd_category = (
    spark.read.csv(
        sep="|",
        header=True,
        inferSchema=True,
        timestampFormat="yyyy-MM-dd",
        path="/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/broadcast_logs/ReferenceTables/CD_Category.csv",
    )
)
display(cd_category)

# COMMAND ----------

cd_program_class = (
    spark.read.csv(
        sep="|",
        header=True,
        inferSchema=True,
        timestampFormat="yyyy-MM-dd",
        path="/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/broadcast_logs/ReferenceTables/CD_ProgramClass.csv",
    )
)
display(cd_program_class)

# COMMAND ----------

full_log = logs_and_channels.join(cd_category,"CategoryID",how = "left").join(cd_program_class,"ProgramClassID", how = "left")
display(full_log.where(F.trim(F.col("ProgramClassCD")).isin(
        ["COM","PRC","PGI","PRO","PSA","MAG","LOC","SPO","MER","SOL"])))

# COMMAND ----------

full_log = full_log.drop(cd_program_class["EnglishDescription"])

# COMMAND ----------

(full_log
 .groupby("ProgramClassCD", "EnglishDescription")
 .agg(F.sum("duration_seconds").alias("duration_total"))
 .orderBy("duration_total", ascending = False).show(100, False)
)

# COMMAND ----------

full_log.selectF.when(
    F.trim(F.col("ProgramClassID")).isin(
        ["COM","PRC","PGI","PRO","PSA","MAG","LOC","SPO","MER","SOL"]
    ),
    F.col("duration_seconds")
).otherwise(0).show()

# COMMAND ----------

answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM","PRC","PGI","PRO","PSA","MAG","LOC","SPO","MER","SOL"]
                ),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio", F.col(
            "duration_commercial") / F.col("duration_total")

        )#.filter(F.col("commercial_ratio") != 1)
    )

display(answer.filter(F.col("commercial_ratio") == 1))
display(answer)

# COMMAND ----------

answer_no_null = answer.dropna(subset=["commercial_ratio"])
display(answer_no_null.orderBy("commercial_ratio", ascending = False))

# COMMAND ----------

answer_no_null_fillna = answer.fillna(0, subset=["commercial_ratio","duration_total" ])
display(answer_no_null_fillna.orderBy("commercial_ratio", ascending = False))
