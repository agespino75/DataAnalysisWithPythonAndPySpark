# Databricks notebook source
book = spark.read.text("/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/gutenberg_books/1342-0.txt")
display(book)

# COMMAND ----------

book.show()

# COMMAND ----------

from pyspark.sql.functions import split, col, explode, lower
#lines = book.select(split(book.value," ").alias("line"))

# COMMAND ----------

book.select(book.value).show()


# COMMAND ----------

from pyspark.sql.functions import col
#book.select(book.value).show()
#book.select(book["value"]).show()
#book.select(col("value")).show(10, truncate=100)
book.select("value").show()

# COMMAND ----------

 book.select(split(col("value")," ").alias("line")).printSchema()

# COMMAND ----------

lines = book.select(split(book.value," "))
#lines = lines.withColumnRenamed("split(value, , -1)","line")

# COMMAND ----------

display(lines)

# COMMAND ----------

lines = lines.withColumnRenamed("split(value,  , -1)","line")

# COMMAND ----------

word = lines.select(explode(col("line")).alias("word"))
word.show(15, truncate=50)

# COMMAND ----------

word_lower = word.select(lower(col("word")).alias("word_lower"))
word_lower.show(15)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract
words_clean = word_lower.select(regexp_extract(col("word_lower"), "[a-z]+",0).alias("word"))
display(words_clean)

# COMMAND ----------

words_nonull = words_clean.filter(col("word") !="")
words_nonull.show(15)

# COMMAND ----------

words_nonull_1 = words_clean.filter(~(col("word") == ""))
words_nonull_1.show(15)

# COMMAND ----------

results = words_nonull.groupby(col("word")).count()
display(results)

# COMMAND ----------

#results.orderBy("count", ascending = False).show(20)
results.orderBy(col("count").desc()).show(50)

# COMMAND ----------

results.coalesce(1).write.csv("/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/simple_count.csv)")

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.appName(
    "Counting word occurences from a book."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# If you need to read multiple text files, replace `1342-0` by `*`.
results = (
    spark.read.text("/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/gutenberg_books/*")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
)

results.orderBy("count", ascending=False).show(10)
results.coalesce(1).write.csv("/Volumes/main/dataanalysiswithpyspark/dataanalysiswithpyspark/DataAnalysisWithPythonAndPySpark-Data-trunk/simple_count.csv)")

