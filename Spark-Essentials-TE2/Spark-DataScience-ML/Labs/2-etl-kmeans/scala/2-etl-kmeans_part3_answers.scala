// Databricks notebook source exported at Mon, 15 Feb 2016 02:08:26 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------
// MAGIC %run /Users/admin@databricks.com/Labs/2-etl-kmeans/scala/2-etl-kmeans_part2_answers

// COMMAND ----------

// MAGIC %md
// MAGIC ## PART 3

// COMMAND ----------

// MAGIC %md
// MAGIC From the class hierarchy it is clear that `KMeans` is an `Estimator` while `KMeansModel` is a `Transformer`.

// COMMAND ----------

import scala.reflect.runtime.universe._
 
def typesOf[T : TypeTag](v: T): List[Type] =
  typeOf[T].baseClasses.map(typeOf[T].baseType)
 
println("*** KMeans instance base classes ***")
typesOf(kmeans).foreach(println)
 
println("\n\n*** KMeansModel base classes ***")
typesOf(model).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's print the three centroids of our model

// COMMAND ----------

centers.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Note that our predicted cluster is appended, as a column, to our input `DataFrame`.  Here it would be desirable to see consistency between label and prediction.  These don't need to be the same number but if label 0 is usually predicted to be cluster 1 that would indicate that our unsupervised learning is naturally grouping the data into species.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Using MLlib instead of ML

// COMMAND ----------

// MAGIC %md
// MAGIC First, convert our `DataFrame` into an `RDD`.

// COMMAND ----------

// Note that .rdd is not necessary, but is here to illustrate that we are working with an RDD
val irisTwoFeaturesRDD = irisTwoFeatures
  .rdd
  .map(r => (r.getAs[Double](1), r.getAs[Vector](0)))
irisTwoFeaturesRDD.take(2)

// COMMAND ----------

import org.apache.spark.sql.Row
// Or alternatively, we could use a pattern match on the Row case class
val irisTwoFeaturesRDD = irisTwoFeatures
  .rdd
  .map { case Row(feature: Vector, label: Double) => (label, feature) }
irisTwoFeaturesRDD.take(2)

// COMMAND ----------

// MAGIC %md
// MAGIC Then import MLlib's `KMeans` as `MLlibKMeans` to differentiate it from `ml.KMeans`

// COMMAND ----------

import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, let's build our k-means model.  Here are the relevant [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.clustering.KMeans) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.clustering.KMeans) APIs.
// MAGIC  
// MAGIC Make sure to set `k` to 3, `maxIterations` to 20, `seed` to 5, and `initializationSteps` to 1.  Also, note that we returned an `RDD` with (label, feature) tuples.  You'll just need the features, which you can obtain by calling `.values()` on `irisTwoFeaturesRDD`.

// COMMAND ----------

// ANSWER
import runtime.ScalaRunTime.stringOf
 
val mllibKMeans = new MLlibKMeans()
  .setK(3)
  .setMaxIterations(20)
  .setSeed(5)
  .setInitializationSteps(1)
  .run(irisTwoFeaturesRDD.values)
 
println(s"mllib: ${stringOf(mllibKMeans.clusterCenters)}")
println(s"ml:    ${stringOf(centers)}")

// COMMAND ----------

// TEST
assert(math.round(mllibKMeans.clusterCenters(0)(0) * 1000) == math.round(centers(0)(0) * 1000),
       "Your mllib and ml models don't match")

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have an `mllibKMeans` model how do we generate predictions and compare those to our labels?

// COMMAND ----------

val predictionsRDD = mllibKMeans.predict(irisTwoFeaturesRDD.values)
predictionsRDD.take(5)

// COMMAND ----------

// MAGIC %md
// MAGIC We'll use `zip` to combine the feature and prediction RDDs together.  Note that zip assumes that the RDDs have the same number of partitions and that each partition has the same number of elements.  This is true here as our predictions were the result of a `map` operation on the feature RDD.

// COMMAND ----------

val combinedRDD = irisTwoFeaturesRDD.zip(predictionsRDD)
combinedRDD.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's compare this to the result from `ml`.

// COMMAND ----------

display(transformed)

// COMMAND ----------

// MAGIC %md
// MAGIC #### How do the `ml` and `mllib` implementations differ?

// COMMAND ----------

// MAGIC %md
// MAGIC The `ml` version of k-means is just a wrapper to MLlib's implementation.  Let's take a look here:
// MAGIC [org.apache.spark.ml.clustering.KMeans source](https://github.com/apache/spark/blob/e1e77b22b3b577909a12c3aa898eb53be02267fd/mllib/src/main/scala/org/apache/spark/ml/clustering/KMeans.scala#L192).
// MAGIC  
// MAGIC How is $ being used in this function? `Param` [source code](https://github.com/apache/spark/blob/2b574f52d7bf51b1fe2a73086a3735b633e9083f/mllib/src/main/scala/org/apache/spark/ml/param/params.scala#L643) has the answer.
// MAGIC  
// MAGIC Which is different than $'s usage for SQL columns where it is a [string interpolator that returns a ColumnName](https://github.com/apache/spark/blob/3d683a139b333456a6bd8801ac5f113d1ac3fd18/sql/core/src/main/scala/org/apache/spark/sql/SQLContext.scala#L386)

// COMMAND ----------

// So $ followed by a string is treated as a custom string interpolator that creates a ColumnName
val num = 10
$"column$num"
