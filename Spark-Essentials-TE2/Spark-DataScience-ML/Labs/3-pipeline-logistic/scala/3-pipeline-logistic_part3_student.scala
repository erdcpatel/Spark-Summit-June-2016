// Databricks notebook source exported at Wed, 10 Feb 2016 23:34:31 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------
// MAGIC %run /Users/admin@databricks.com/Labs/3-pipeline-logistic/scala/3-pipeline-logistic_part2_answers

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 3

// COMMAND ----------

// MAGIC %md
// MAGIC Let's just check and see that our norms are equal to 1.0

// COMMAND ----------

import org.apache.spark.mllib.linalg.{Vector, Vectors}
 
val l2Norm = udf { v: Vector => Vectors.norm(v, 2.0) }
 
val featureLengths = irisNormalized.select(l2Norm($"features").alias("featuresLength"),
                                           l2Norm($"featureNorm").alias("featureNormLength"))
display(featureLengths)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's bucketize our features.  This will allow us to convert continuous features into discrete buckets.  This is often desirable for logistic regression which we'll be performing later in this lab.
// MAGIC  
// MAGIC We'll use the following splits: -infinity, -0.5, 0.0, 0.5, +infinity.  Note that in Python infinity can be represented using `float('inf')` and that in Scala `Double.NegativeInfinity` and `Double.PositiveInfinity` can be used.

// COMMAND ----------

import org.apache.spark.ml.feature.Bucketizer
 
val splits = Array(Double.NegativeInfinity, -.5, 0.0, .5, Double.PositiveInfinity)
 
val lengthBucketizer = new Bucketizer()
  .setInputCol("sepalLength")
  .setOutputCol("lengthFeatures")
  .setSplits(splits)
 
val irisBucketizedLength = lengthBucketizer.transform(irisSeparateFeatures)
display(irisBucketizedLength)

// COMMAND ----------

val widthBucketizer = new Bucketizer()
  .setInputCol("sepalWidth")
  .setOutputCol("widthFeatures")
  .setSplits(splits)
 
val irisBucketizedWidth = widthBucketizer.transform(irisBucketizedLength)
display(irisBucketizedWidth)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's combine the two bucketizers into a [Pipeline](http://spark.apache.org/docs/latest/ml-guide.html#pipeline-components) that performs both bucketizations.  A `Pipeline` is made up of stages which can be set using `setStages` and passing in a `list` of stages in Python or an `Array` of stages in Scala.  `Pipeline` is an estimator, which means it implements a `fit` method which returns a `PipelineModel`.  A `PipelineModel` is a transformer, which means that it implements a `transform` method which can be used to run the stages.

// COMMAND ----------

import org.apache.spark.ml.Pipeline
 
val pipelineBucketizer = new Pipeline().setStages(Array(lengthBucketizer, widthBucketizer))
 
val pipelineModelBucketizer = pipelineBucketizer.fit(irisSeparateFeatures)
val irisBucketized = pipelineModelBucketizer.transform(irisSeparateFeatures)
 
display(irisBucketized)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have created two new features through bucketing, let's combine those two features into a `Vector` with `VectorAssembler`.  VectorAssembler can be found in [pyspark.ml.feature](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler) for Python and the [org.apache.spark.ml.feature](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler) package for Scala.
// MAGIC  
// MAGIC Set the params of `assembler` so that both "lengthFeatures" and "widthFeatures" are assembled into a column called "featuresBucketized".
// MAGIC  
// MAGIC Then, set the stages of `pipeline` to include both bucketizers and the assembler as the last stage.
// MAGIC  
// MAGIC Finally, use `pipeline` to generate a new `DataFrame` called `irisAssembled`.

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
val pipeline = new Pipeline()
val assembler = new VectorAssembler()
 
println(assembler.explainParams())
println("\n" + pipeline.explainParams() + "\n\n")

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
// Set assembler params
assembler
  <FILL IN>
  <FILL IN>
 
pipeline.<FILL IN>
val irisAssembled = <FILL IN>
display(irisAssembled)

// COMMAND ----------

// TEST
import org.apache.spark.mllib.linalg.Vectors
val firstAssembly = irisAssembled.select("lengthFeatures", "widthFeatures", "featuresBucketized").first
assert(Vectors.dense(Array(firstAssembly.getAs[Double](0), firstAssembly.getAs[Double](1))) == firstAssembly(2),
       "incorrect value for column featuresBucketized")
