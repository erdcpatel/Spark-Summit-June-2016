// Databricks notebook source exported at Wed, 10 Feb 2016 23:34:31 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------
// MAGIC %run /Users/admin@databricks.com/Labs/3-pipeline-logistic/scala/3-pipeline-logistic_part1_answers

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 2

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's register our function and then call it directly from SQL.

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vector
 
sqlContext.udf.register("getElement", getElement.f.asInstanceOf[Function2[Vector, Int, Double]])
irisTwoFeatures.registerTempTable("irisTwo")

// COMMAND ----------

// MAGIC %sql
// MAGIC select getElement(features, 0) as sepalLength from irisTwo

// COMMAND ----------

// MAGIC %md
// MAGIC #### EDA and feature engineering

// COMMAND ----------

// MAGIC %md
// MAGIC Let's see the ranges of our values and view their means and standard deviations.

// COMMAND ----------

// MAGIC %md
// MAGIC Our features both take on values from -1.0 to 1.0, but have different means and standard deviations.  How could we standardize our data to have zero mean and unit standard deviations?  For this task we'll use the `ml` estimator `StandardScaler`.  Feature transformers (which are sometimes estimators) can be found in [pyspark.ml.feature](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature) for Python or [org.apache.spark.ml.feature](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.package) for Scala.
// MAGIC  
// MAGIC Also, remember that the [ML Guide](http://spark.apache.org/docs/latest/ml-features.html#standardscaler) is a good place to find additional information.

// COMMAND ----------

import org.apache.spark.ml.feature.StandardScaler
 
val standardScaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("standardized")
  .setWithMean(true)
 
println(standardScaler.explainParams() + "\n\n")

// COMMAND ----------

val irisStandardizedLength = standardScaler
  .fit(irisSeparateFeatures)
  .transform(irisSeparateFeatures)
  .withColumn("standardizedLength", getElement($"standardized", lit(0)))
display(irisStandardizedLength)

// COMMAND ----------

display(irisStandardizedLength.describe("sepalLength", "standardizedLength"))

// COMMAND ----------

// MAGIC %md
// MAGIC What if instead we wanted to normalize the data?  For example, we might want to normalize each set of features (per row) to have length one using an \\( l^2 \\) norm.  That would cause the sum of the features squared to be one: \\( \sum_{i=1}^d x_i^2 = 1 \\).  This is could be useful if we wanted to compare observations based on a distance metric like in k-means clustering.
// MAGIC  
// MAGIC `Normalizer` can be found in [pyspark.ml.feature](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Normalizer) for Python and the [org.apache.spark.ml.feature](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.Normalizer) package for Scala.
// MAGIC  
// MAGIC Let's implement `Normalizer` and transform our features.  Make sure to use a `P` of 2.0 and to name the output column "featureNorm".  Remember that we're working with the `irisTwoFeatures` dataset.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
import org.apache.spark.ml.feature.Normalizer
 
val normalizer = new Normalizer()
  .setInputCol("features")
  .setOutputCol("featureNorm")
  .setP(2.0)
 
val irisNormalized = normalizer.transform(irisTwoFeatures)  // Note that we're calling transform here
display(irisNormalized)

// COMMAND ----------

// TEST
import org.apache.spark.mllib.linalg.Vectors
val firstVector = irisNormalized.select("featureNorm").map(_.getAs[Vector](0)).first
val vectorNorm = Vectors.norm(firstVector, 2.0)
 
assert(math.round(vectorNorm) == 1.0,
       "incorrect setup of normalizer")
