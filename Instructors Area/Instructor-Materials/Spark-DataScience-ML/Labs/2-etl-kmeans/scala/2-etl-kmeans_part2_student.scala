// Databricks notebook source exported at Mon, 15 Feb 2016 02:08:26 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------
// MAGIC %run /Users/admin@databricks.com/Labs/2-etl-kmeans/scala/2-etl-kmeans_part1_answers

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 2

// COMMAND ----------

import org.apache.spark.ml.feature.VectorSlicer
val vs = new VectorSlicer()
  .setInputCol("features")
  .setOutputCol("twoFeatures")
  .setIndices(Array(0, 1))
val irisTwoFeaturesSlicer = vs.transform(irisDFZeroIndex)
  .select($"twoFeatures".as("features"), $"label")

// COMMAND ----------

// VectorSlicer returns a SparseVector
irisTwoFeaturesSlicer.first().getAs[Vector](0)

// COMMAND ----------

// Let's create a UDF to generate a DenseVector instead
val toDense = udf { (vector: Vector) => vector.toDense }
 
val irisTwoFeatures = irisTwoFeaturesSlicer.select(toDense($"features").as("features"), $"label")
irisTwoFeatures.first.getAs[Vector](0)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's view our `irisTwoFeatures` `DataFrame`.

// COMMAND ----------

irisTwoFeatures.take(5).mkString("\n")

// COMMAND ----------

display(irisTwoFeatures)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Saving our DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC We'll be using parquet files to save our data.  More information about the parquet file format can be found on [parquet.apache.org](https://parquet.apache.org/documentation/latest/).

// COMMAND ----------

import scala.util.Random
val id = Random.nextLong.toString
irisTwoFeatures.write.mode("overwrite").parquet(s"/tmp/$id/irisTwoFeatures.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC Note that we'll get a part file for each partition and that these files are compressed.

// COMMAND ----------

//display(dbutils.fs.ls(baseDir + "irisTwoFeatures.parquet"))
display(dbutils.fs.ls(s"/tmp/$id/irisTwoFeatures.parquet"))

// COMMAND ----------

irisDFZeroIndex.write.mode("overwrite").parquet(s"/tmp/$id/irisFourFeatures.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC #### K-Means

// COMMAND ----------

// MAGIC %md
// MAGIC Now we'll build a k-means model using our two features and inspect the class hierarchy.
// MAGIC  
// MAGIC We'll build the k-means model using `KMeans`, an `ml` `Estimator`.  Details can be found in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.clustering) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.clustering.package) APIs.  Also, examples that use [PCA](http://spark.apache.org/docs/latest/ml-features.html#pca) and  [logistic regression](http://spark.apache.org/docs/latest/ml-guide.html#example-estimator-transformer-and-param) can be found in the ML Programming Guide.
// MAGIC  
// MAGIC Make sure to work with the `irisTwoFeatures` `DataFrame`.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
import org.apache.spark.ml.clustering.KMeans
 
// Create a KMeans Estimator and set k=3, seed=5, maxIter=20, initSteps=1
val kmeans = <FILL IN>
  <FILL IN>
  <FILL IN>
  <FILL IN>
  <FILL IN>
 
// Call fit on the estimator and pass in our DataFrame
val model = <FILL IN>
 
// Obtain the clusterCenters from the KMeansModel
val centers = <FILL IN>
 
// Use the model to transform the DataFrame by adding cluster predictions
val transformed = <FILL IN>

// COMMAND ----------

import org.apache.spark.ml.clustering.KMeans
 
// Create a KMeans Estimator and set k=3, seed=5, maxIter=20, initSteps=1
val kmeans = new KMeans()
  .setK(3)
  .setSeed(5)
  .setMaxIter(20)
  .setInitSteps(1)
 
// Call fit on the estimator and pass in our DataFrame
val model = kmeans.fit(irisTwoFeatures)
 
// Obtain the clusterCenters from the KMeansModel
val centers = model.clusterCenters
 
// Use the model to transform the DataFrame by adding cluster predictions
val transformed = model.transform(irisTwoFeatures)

// COMMAND ----------

// TEST
import org.apache.spark.mllib.linalg.Vectors
 
assert(math.round(centers(0)(0)* 1000) == 351,
       "incorrect centers.  check your params.")
assert(transformed.select("prediction").map(_(0)).take(4).deep == Array(1,1,1,1).deep,
       "incorrect predictions")
