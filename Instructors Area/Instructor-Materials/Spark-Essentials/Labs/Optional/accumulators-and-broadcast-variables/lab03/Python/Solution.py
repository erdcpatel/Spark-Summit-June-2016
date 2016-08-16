# Databricks notebook source exported at Wed, 4 Nov 2015 20:34:50 UTC
# MAGIC %md
# MAGIC # Answers: Custom Accumulators Lab

# COMMAND ----------

# MAGIC %md ### Exercise Part 1

# COMMAND ----------

class AccumWordCounts(AccumulatorParam):
    def zero(self, initial):
      return set()

    def addInPlace(self, s1, s2):
      return s1 | s2

# COMMAND ----------

# MAGIC %md ### Exercise Part 2

# COMMAND ----------

def update_set(i):
  global unique_numbers
  unique_numbers += set([i])

rdd.foreach(update_set)

# COMMAND ----------

