// Databricks notebook source exported at Wed, 10 Feb 2016 23:34:31 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------
// MAGIC %run /Users/admin@databricks.com/Labs/3-pipeline-logistic/scala/3-pipeline-logistic_part4_answers

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 5

// COMMAND ----------

import scala.runtime.ScalaRunTime.stringOf
println(stringOf(pipelineModelLR.stages))
println(pipelineModelLR.stages.last.asInstanceOf[LogisticRegressionModel].weights)

// COMMAND ----------

// MAGIC %md
// MAGIC Leaving our features to range from 0 to 3 means that a value of 2 has twice the impact in our model than a value of 1.  Since these buckets were based on increasing numeric values this is not unreasonable; however, we might want to convert each of these values to a dummy feature that takes on either a 0 or 1 corresponding to whether the value occurs.  This allows the model to measure the impact of the occurrences of the individual values and allows for non-linear relationships.
// MAGIC  
// MAGIC To do this we'll use the `OneHotEncoder` estimator.

// COMMAND ----------

import org.apache.spark.ml.feature.OneHotEncoder
 
val oneHotLength = new OneHotEncoder()
  .setInputCol("lengthFeatures")
  .setOutputCol("lengthOneHot")
 
pipeline.setStages(Array(lengthBucketizer, widthBucketizer, oneHotLength))
 
val irisWithOneHotLength = pipeline.fit(irisTrain).transform(irisTrain)
display(irisWithOneHotLength)

// COMMAND ----------

irisWithOneHotLength.select("lengthOneHot").first

// COMMAND ----------

// MAGIC %md
// MAGIC Create a `OneHotEncoder` for width as well, and combine both encoders together into a `featuresBucketized` column.

// COMMAND ----------

val oneHotWidth = new OneHotEncoder()
  .setInputCol("widthFeatures")
  .setOutputCol("widthOneHot")
 
val assembleOneHot = new VectorAssembler()
  .setInputCols(Array("lengthOneHot", "widthOneHot"))
  .setOutputCol("featuresBucketized")
 
pipeline.setStages(Array(lengthBucketizer, widthBucketizer, oneHotLength, oneHotWidth, assembleOneHot))
 
display(pipeline.fit(irisTrain).transform(irisTrain))

// COMMAND ----------

// MAGIC %md
// MAGIC Create the full `Pipeline` through logistic regression and make predictions on the test data.

// COMMAND ----------

pipeline.setStages(Array(lengthBucketizer, widthBucketizer, oneHotLength, oneHotWidth, assembleOneHot, lr))
 
val pipelineModelLR2 = pipeline.fit(irisTrain)
 
val irisTestPredictions2 = pipelineModelLR2
  .transform(irisTest)
  .cache()
display(irisTestPredictions2)

// COMMAND ----------

// MAGIC %md
// MAGIC What does our new model look like?

// COMMAND ----------

val logisticModel = pipelineModelLR2.stages.last.asInstanceOf[LogisticRegressionModel]
println(logisticModel.intercept)
println(stringOf(logisticModel.weights))

// COMMAND ----------

// MAGIC %md
// MAGIC What about model accuracy?

// COMMAND ----------

import org.apache.spark.sql.DataFrame
 
def modelAccuracy(df: DataFrame) = {
  df
  .select(($"prediction" === $"label").cast("int").alias("correct"))
  .groupBy()
  .avg("correct")
  .first()(0)
}
 
val modelOneAccuracy = modelAccuracy(irisTestPredictions)
val modelTwoAccuracy = modelAccuracy(irisTestPredictions2)
 
println(s"modelOneAccuracy: $modelOneAccuracy")
println(s"modelTwoAccuracy: $modelTwoAccuracy\n\n")

// COMMAND ----------

// MAGIC %md
// MAGIC Or we can use SQL instead.

// COMMAND ----------

irisTestPredictions.registerTempTable("modelOnePredictions")
val sqlResult = sqlContext.sql("select avg(int(prediction == label)) from modelOnePredictions")
display(sqlResult)

// COMMAND ----------

// MAGIC %md
// MAGIC An even better option is to use the tools already built-in to Spark.  The MLlib guide has a lot of information regarding [evaluation metrics](http://spark.apache.org/docs/latest/mllib-evaluation-metrics.html).  For ML, you can find details in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation) and [Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.package) APIs.
// MAGIC  
// MAGIC A common metric used for logistic regression is area under the ROC curve (AUC).  We can use the `BinaryClasssificationEvaluator` to obtain the AUC for our two models.  Make sure to set the metric to "areaUnderROC" and that you set the rawPrediction column to "rawPrediction".
// MAGIC  
// MAGIC Recall that `irisTestPredictions` are the test predictions from our first model and `irisTestPredictions2` are the test predictions from our second model.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
 
val binaryEvaluator = <FILL IN>
  .<FILL IN>
  .<FILL IN>
 
val firstModelTestAUC = binaryEvaluator.evaluate(irisTestPredictions)
val secondModelTestAUC = binaryEvaluator.evaluate(irisTestPredictions2)
 
println(s"First model AUC: $firstModelTestAUC")
println(s"Second model AUC: $secondModelTestAUC")
 
val irisTrainPredictions = pipelineModelLR.transform(irisTrain)
val irisTrainPredictions2 = pipelineModelLR2.transform(irisTrain)
 
val firstModelTrainAUC = binaryEvaluator.evaluate(irisTrainPredictions)
val secondModelTrainAUC = binaryEvaluator.evaluate(irisTrainPredictions2)
 
println(s"\nFirst model training AUC: $firstModelTrainAUC")
println(s"Second model training AUC: $secondModelTrainAUC\n\n")

// COMMAND ----------

assert(firstModelTestAUC > .95, "incorrect firstModelTestAUC")
assert(secondModelTrainAUC > .95, "incorrect secondMOdelTrainAUC")

// COMMAND ----------

// MAGIC %md
// MAGIC **Visualization: ROC curve **
// MAGIC  
// MAGIC We will now visualize how well the model predicts our target.  To do this we generate a plot of the ROC curve.  The ROC curve shows us the trade-off between the false positive rate and true positive rate, as we liberalize the threshold required to predict a positive outcome.  A random model is represented by the dashed line.

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
 
val metric ="precision"
 
val multiclassEval = new MulticlassClassificationEvaluator()
 
multiclassEval.setMetricName(metric)
println(s"Model one $metric: ${multiclassEval.evaluate(irisTestPredictions)}")
println(s"Model two $metric: ${multiclassEval.evaluate(irisTestPredictions2)}\n")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Using MLlib instead of ML
// MAGIC  
// MAGIC We've been using `ml` transformers, estimators, pipelines, and evaluators.  How can we accomplish the same things with MLlib?

// COMMAND ----------

irisTestPredictions.columns

// COMMAND ----------

irisTestPredictions.take(1)

// COMMAND ----------

// MAGIC %md
// MAGIC Pull the data that we need from our `DataFrame` and create `BinaryClassificationMetrics`.

// COMMAND ----------

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
 
val modelOnePredictionLabel = irisTestPredictions
  .select("rawPrediction", "label")
  .rdd
  .map(r => (r.getAs[DenseVector](0)(1), r.getAs[Double](1)))
 
val modelTwoPredictionLabel = irisTestPredictions2
  .select("rawPrediction", "label")
  .rdd
  .map(r => (r.getAs[DenseVector](0)(1), r.getAs[Double](1)))
 
val metricsOne = new BinaryClassificationMetrics(modelOnePredictionLabel)
val metricsTwo = new BinaryClassificationMetrics(modelTwoPredictionLabel)
 
println(metricsOne.areaUnderROC)
println(metricsTwo.areaUnderROC)

// COMMAND ----------

// MAGIC %md
// MAGIC To build a logistic regression model with MLlib we'll need the data to be an RDD of `LabeledPoints`.  For testing purposes we'll pull out the label and features into a tuple, since we'll want to make predictions directly on the features and not on a `LabeledPoint`.

// COMMAND ----------

import org.apache.spark.mllib.regression.LabeledPoint
 
val irisTrainRDD = irisTrainPredictions
  .select("label", "featuresBucketized")
  .map(r => LabeledPoint(r.getAs[Double](0), r.getAs[Vector](1)))
  .cache
 
val irisTestRDD = irisTestPredictions
  .select("label", "featuresBucketized")
  .map(r => (r.getAs[Double](0), r.getAs[Vector](1)))
  .cache
 
irisTrainRDD.take(2).foreach(println)
irisTestRDD.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Now, we can use MLlib's logistic regression on our `RDD` of `LabeledPoints`.  Note that we'll use `LogisticRegressionWithLBFGS` as it tends to converge faster than `LogisticRegressionWithSGD`.

// COMMAND ----------

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
 
val mllibModel = new LogisticRegressionWithLBFGS()
  .run(irisTrainRDD)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's calculate our accuracy using `RDDs`.

// COMMAND ----------

val rddPredictions = mllibModel.predict(irisTestRDD.values)
val predictAndLabels = rddPredictions.zip(irisTestRDD.keys)
 
val mllibAccuracy = predictAndLabels.map( x => if (x._1 == x._2) 1 else 0).mean()
println(s"MLlib model accuracy: $mllibAccuracy")
