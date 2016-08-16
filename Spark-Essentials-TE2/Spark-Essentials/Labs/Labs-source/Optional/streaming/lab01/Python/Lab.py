# Databricks notebook source exported at Tue, 28 Jul 2015 16:25:41 UTC
# MAGIC %md
# MAGIC # Spark Streaming Lab (Python)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC This first section is largely just setup. If you want to play around with the configuration constants, feel free to do so. But leave the imports and the `assert` alone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## WARNING! THIS LAB DOESN'T WORK YET!

# COMMAND ----------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import random

WORDS_FILE = "dbfs:/home/training/streaming/randomized-word-game.txt"
TABLE_NAME = "words_{0}".format(random.randint(1, 1000))



# COMMAND ----------

ssc = StreamingContext(sc, 1)

# COMMAND ----------

stream = ssc.textFileStream('dbfs:/home/training/streaming')
word_count_stream = stream.map(lambda word: (word, 1)).reduceByKey(lambda m, n: m + n)
word_count_stream.foreachRDD(lambda rdd: rdd.toDF("word, count").write.mode("append").saveAsTable(TABLE_NAME))

# COMMAND ----------

print(TABLE_NAME)
ssc.start()
ssc.awaitTermination()

# COMMAND ----------

