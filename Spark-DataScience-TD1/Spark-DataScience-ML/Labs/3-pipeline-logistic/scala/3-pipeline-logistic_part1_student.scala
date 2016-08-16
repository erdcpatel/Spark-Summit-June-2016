// Databricks notebook source exported at Wed, 10 Feb 2016 23:34:31 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Pipelines and Logistic Regression
// MAGIC  
// MAGIC In this lab we'll cover transformers, estimators, evaluators, and pipelines.  We'll use transformers and estimators to prepare our data for use in a logistic regression model and will use pipelines to combine these steps together.  Finally, we'll evaluate our model.
// MAGIC  
// MAGIC This lab also covers creating train and test datasets using `randomSplit`, visualizing a ROC curve, and generating both `ml` and `mllib` logistic regression models.
// MAGIC  
// MAGIC After completing this lab you should be comfortable using transformers, estimators, evaluators, and pipelines.

// COMMAND ----------

val baseDir = "/mnt/ml-class/"
val irisTwoFeatures = sqlContext.read.parquet(baseDir + "irisTwoFeatures.parquet").cache
irisTwoFeatures.take(2).foreach(println)

// COMMAND ----------

display(irisTwoFeatures)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Prepare the data
// MAGIC  
// MAGIC To explore our data in more detail, we're going to we pull out sepal length and sepal width and create two columns.  These are the two features found in our `DenseVector`.
// MAGIC  
// MAGIC In order to do this you will write a `udf` that takes in two values.  The first will be the name of the vector that we are operating on and the second is a literal for the index position.  Here are links to `lit` in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.lit) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) APIs.
// MAGIC  
// MAGIC The `udf` will return a `DoubleType` that is the value of the specified vector at the specified index position.
// MAGIC  
// MAGIC In order to call our function, we need to wrap the second value in `lit()` (e.g. `lit(1)` for the second element).  This is because our `udf` expects a `Column` and `lit` generates a `Column` where the literal is the value.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
import org.apache.spark.sql.functions.{udf, lit}
import org.apache.spark.mllib.linalg.Vector
 
val getElement = udf { <FILL IN> }
 
val irisSeparateFeatures = (irisTwoFeatures
                            .withColumn("sepalLength", getElement($"features", lit(0)))
                            .withColumn("sepalWidth", getElement($"features", <FILL IN>)))
display(irisSeparateFeatures)

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vector
// TEST
val firstRow = irisSeparateFeatures.select("sepalWidth", "features").map(r => (r.getAs[Double](0), r.getAs[Vector](1))).first
assert(firstRow._1 == firstRow._2(1), "incorrect definition for getElement")

// COMMAND ----------

// MAGIC %md
// MAGIC What about using `Column`'s `getItem` method?

// COMMAND ----------

try {
  display(irisTwoFeatures.withColumn("sepalLength", $"features".getItem(0)))
} catch {
  case ae: org.apache.spark.sql.AnalysisException => println(ae)
}

// COMMAND ----------

// MAGIC %md
// MAGIC Unfortunately, it doesn't work for vectors, but it does work on arrays.

// COMMAND ----------

val arrayDF = sqlContext.createDataFrame(Seq((Array(1,2,3), 0), (Array(3,4,5), 1))).toDF("anArray", "index")
arrayDF.show
 
arrayDF.select($"anArray".getItem(0)).show()
arrayDF.select($"anArray"(1)).show()
