// Databricks notebook source exported at Fri, 22 Apr 2016 17:20:02 UTC
// MAGIC %md # Catalyst + Tungsten

// COMMAND ----------

sqlContext.read.text("dbfs:/mnt/training/data/tom_sawyer").collect

// COMMAND ----------

sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word").collect


// COMMAND ----------

// MAGIC %md ## Optimizations

// COMMAND ----------

sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word").filter("word > 'F'").filter("word > 'H'").explain(true)

// COMMAND ----------

sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word").select("word").filter("word > 'H'").explain(true)

// COMMAND ----------

sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word").filter("word > 'F'").select("word").filter("word > 'H'").explain(true)

// COMMAND ----------

// MAGIC %md ## Exchanges, Jobs, and Shuffles (Stages)

// COMMAND ----------

sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word").collect

// COMMAND ----------

sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word").groupBy("word").count.collect

// COMMAND ----------

sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word").groupBy("word").count.orderBy($"count".desc).collect

// COMMAND ----------

sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word")
  .groupBy("word").count.withColumnRenamed("count", "wordOccurrences").groupBy("wordOccurrences").count.orderBy("wordOccurrences").collect

// COMMAND ----------

val df = sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer").flatMap(_.getString(0).split(" ")).toDF("word")
df.cache
df.groupBy("word").count.withColumnRenamed("count", "wordOccurrences").groupBy("wordOccurrences").count.orderBy("wordOccurrences").collect

// COMMAND ----------

val df = sqlContext.read.text("dbfs:///mnt/training/data/tom_sawyer")

// Interpret each line as a java.lang.String (using built-in encoder)
val ds = df.as[String]
.flatMap(line => line.split(" "))               // Split on whitespace
.filter(line => line != "")                     // Filter empty words
.groupBy(word => word)
.count()

ds.collect

// COMMAND ----------

