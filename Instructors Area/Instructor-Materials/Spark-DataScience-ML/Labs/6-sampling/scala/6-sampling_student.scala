// Databricks notebook source exported at Wed, 10 Feb 2016 23:35:05 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Sampling
// MAGIC  
// MAGIC This lab demonstrates how to perform sampling including stratified sampling.  There are examples using both `DataFrame` and `RDD` operations

// COMMAND ----------

val baseDir = "/mnt/ml-class/"
val irisTwoFeatures = sqlContext.read.parquet(baseDir + "irisTwoFeatures.parquet")

// COMMAND ----------

display(irisTwoFeatures)

// COMMAND ----------

// MAGIC %md
// MAGIC When using a `DataFrame` we can call `.sampleBy` to return a stratified sample without using replacement.  `sampleBy` takes in a column and fractions for what percentage of each value to sample.  An explanation of `sampleBy` can be found under [DataFrame](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sampleBy) for the Python API and under [DataFrameStatFunctions](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrameStatFunctions) for the Scala API.

// COMMAND ----------

val stratifiedSample = irisTwoFeatures.stat.sampleBy("label", Map(0 -> .10, 1 -> .20, 2 -> .3), 0)
display(stratifiedSample)

// COMMAND ----------

// MAGIC %md
// MAGIC How many?  And which labels did we sample?

// COMMAND ----------

println(s"total count: ${stratifiedSample.count()}")

// COMMAND ----------

val labelCounts = stratifiedSample
  .groupBy("label")
  .count
  .orderBy("label")
display(labelCounts)

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's sample with replacement from the `DataFrame`.

// COMMAND ----------

val sampleWithReplace = irisTwoFeatures.sample(true, .20)
val labelCountsReplace = sampleWithReplace
  .groupBy("label")
  .count
  .orderBy("label")
display(labelCountsReplace)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Convert to an RDD and sample from an RDD

// COMMAND ----------

// MAGIC %md
// MAGIC First, we'll convert our `DataFrame` to an `RDD`.

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vector
 
val irisTwoFeaturesRDD = irisTwoFeatures
  .rdd
  .map(r => (r.getAs[Double](1), r.getAs[Vector](0)))
 
irisTwoFeaturesRDD.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll perform stratified sampling.

// COMMAND ----------

val irisSampleRDD = irisTwoFeaturesRDD.sampleByKey(true, Map(0.0 -> .5, 1.0 -> .5, 2.0 -> .1), 1)
 
irisSampleRDD.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC What do our counts look like?

// COMMAND ----------

println(irisTwoFeaturesRDD.countByKey)
println(irisSampleRDD.countByKey)

// COMMAND ----------

// MAGIC %md
// MAGIC We could also call `sample` to perform a random sample instead of a stratified sample.
