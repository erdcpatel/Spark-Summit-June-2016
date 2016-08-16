// Databricks notebook source exported at Mon, 15 Feb 2016 02:40:09 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Decision Trees
// MAGIC  
// MAGIC This lab covers decision trees and random forests, while introducing metadata, cross-validation, `StringIndexer`, and `PolynomialExpansion`.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Prepare the data

// COMMAND ----------

// MAGIC %md
// MAGIC Load in the data from a parquet file.

// COMMAND ----------

val baseDir = "/mnt/ml-class/"
val irisFourFeatures = sqlContext.read.parquet(baseDir + "irisFourFeatures.parquet")
irisFourFeatures.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Convert the data from `SparseVector` to `DenseVector` types.

// COMMAND ----------

import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.linalg.Vector
 
val sparseToDense = udf { sv: Vector => sv.toDense }
val irisDense = irisFourFeatures.select(sparseToDense($"features").as("features"), $"label")
 
irisDense.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Save the new format for use in another notebook.

// COMMAND ----------

//irisDense.write.mode("overwrite").parquet("/tmp/irisDense.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC Split the data into train and test sets and visualize the datasets.

// COMMAND ----------

val Array(irisTest, irisTrain) = irisDense.randomSplit(Array(.30, .70), seed=1)
irisTest.cache()
irisTrain.cache()
 
println(s"Items in test datset: ${irisTest.count()}")
println(s"Items in train datset: ${irisTrain.count()}")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Update the metadata for decision trees and build a tree

// COMMAND ----------

// MAGIC %md
// MAGIC We use `StringIndexer` on our labels in order to obtain a `DataFrame` that decision trees can work with.  Here are the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.StringIndexer) APIs for `StringIndexer`.
// MAGIC  
// MAGIC You'll need to set the input column to "label", the output column to "indexed", and fit and transform using the `irisTrain` `DataFrame`.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
import org.apache.spark.ml.feature.StringIndexer
 
val stringIndexer = <FILL IN>
  <FILL IN>
  <FILL IN>
 
val indexerModel = stringIndexer.<FILL IN>
val irisTrainIndexed = indexerModel.<FILL IN>
display(irisTrainIndexed)

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer
 
val stringIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexed")
 
val indexerModel = stringIndexer.fit(irisTrain)
val irisTrainIndexed = indexerModel.transform(irisTrain)
display(irisTrainIndexed)

// COMMAND ----------

// TEST
assert(irisTrainIndexed.select("indexed").take(50).last(0) == 2.0, "incorrect values in indexed column")
assert(irisTrainIndexed.schema.fields(1).metadata != irisTrainIndexed.schema.fields(2).metadata,
       "field metadata should not be the same")

// COMMAND ----------

// MAGIC %md
// MAGIC We've updated the metadata for the field.  Now we know that the field takes on three values and is nominal.

// COMMAND ----------

println(irisTrainIndexed.schema.fields(1).metadata)
println(irisTrainIndexed.schema.fields(2).metadata)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's build a decision tree to classify our data.

// COMMAND ----------

import org.apache.spark.ml.classification.DecisionTreeClassifier
 
val dt = new DecisionTreeClassifier()
  .setLabelCol("indexed")
  .setMaxDepth(5)
  .setMaxBins(10)
  .setImpurity("gini")

// COMMAND ----------

println(dt.explainParam(dt.impurity) + "\n")
println(dt.explainParam(dt.maxBins))

// COMMAND ----------

// MAGIC %md
// MAGIC View all of the parameters to see if there is anything we'd like to update.

// COMMAND ----------

println(dt.explainParams)

// COMMAND ----------

// MAGIC %md
// MAGIC Fit the model and display predictions on the test data.

// COMMAND ----------

val dtModel = dt.fit(irisTrainIndexed)
val predictionsTest = dtModel.transform(indexerModel.transform(irisTest))
display(predictionsTest)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll evaluate the results of the model.

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
 
val multiEval = new MulticlassClassificationEvaluator()
  .setMetricName("precision")
  .setLabelCol("indexed")
 
println(multiEval.evaluate(predictionsTest))

// COMMAND ----------

// MAGIC %md
// MAGIC View the decision tree model.

// COMMAND ----------

val dtModelString = dtModel.toDebugString
println(dtModelString)

// COMMAND ----------

var readableModel = dtModelString
 
for ((name, feature) <- Array("Sepal Length", "Sepal Width", "Petal Length", "Petal Width").zipWithIndex)
  readableModel = readableModel.replace(s"feature $feature", name)
 
println(readableModel)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Cross-validation

// COMMAND ----------

// MAGIC %md
// MAGIC Let's go ahead and find the best cross-validated model.  A `CrossValidator` requires an estimator to build the models, an evaluator to compare the performance of the models, a parameter grid that specifies which estimator parameters to tune, and the number of folds to use.
// MAGIC  
// MAGIC There is a good example in the [ML Guide](http://spark.apache.org/docs/latest/ml-guide.html#example-model-selection-via-cross-validation), although it is only in Scala.  The Python code is very similar.
// MAGIC  
// MAGIC The estimator that we will use is a `Pipeline` that has `stringIndexer` and `dt`.
// MAGIC  
// MAGIC The evaluator will be `multiEval`.  You just need to make sure the metric is "precision".
// MAGIC  
// MAGIC We'll use `ParamGridBuilder` to build a grid with `dt.maxDepth` values of 2, 4, 6, and 10 (in that order).
// MAGIC  
// MAGIC Finally, we'll use 5 folds.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.Pipeline
 
val cvPipeline = new Pipeline().setStages(<FILL IN>)
 
multiEval.setMetricName(<FILL IN>)
 
val paramGrid = <FILL IN>
  .<FILL IN>
  .<FILL IN>
 
val cv = <FILL IN>
  .<FILL IN>
  .<FILL IN>
  .<FILL IN>
  .<FILL IN>
 
val cvModel = cv.fit(irisTrain)
val avgMetrics = cvModel.avgMetrics
paramGrid.zip(avgMetrics).foreach(println)
println("\n\n\n")

// COMMAND ----------

// TEST
assert(math.round(avgMetrics(0) * 1000) == 920, "incorrect avgMetrics")

// COMMAND ----------

// MAGIC %md
// MAGIC What was our best model?

// COMMAND ----------

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
 
val bestDTModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[DecisionTreeClassificationModel]
println(bestDTModel)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's see more details on what parameters were used to build the best model.

// COMMAND ----------

println(bestDTModel.parent.explainParams)

// COMMAND ----------

println(bestDTModel.parent.asInstanceOf[DecisionTreeClassifier].getMaxDepth)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Random forest and `PolynomialExpansion`
// MAGIC  
// MAGIC Next, we'll build a random forest.  Since we only have a few features and random forests tend to work better with a lot of features, we'll expand our features using `PolynomialExpansion`.

// COMMAND ----------

import org.apache.spark.ml.feature.PolynomialExpansion
 
val px = new PolynomialExpansion()
  .setInputCol("features")
  .setOutputCol("polyFeatures")
 
println(px.explainParams)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll use the `RandomForestClassifier` to build our random forest model.

// COMMAND ----------

import org.apache.spark.ml.classification.RandomForestClassifier
 
val rf = new RandomForestClassifier()
  .setLabelCol("indexed")
  .setFeaturesCol("polyFeatures")
 
println(rf.explainParams)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's set some params based on what we just read.

// COMMAND ----------

rf.setMaxBins(10)
  .setMaxDepth(2)
  .setNumTrees(20)
  .setSeed(0)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll build a pipeline that includes the `StringIndexer`, `PolynomialExpansion`, and `RandomForestClassifier`.

// COMMAND ----------

val rfPipeline = new Pipeline().setStages(Array(stringIndexer, px, rf))
val rfModelPipeline = rfPipeline.fit(irisTrain)
val rfPredictions = rfModelPipeline.transform(irisTest)
 
println(multiEval.evaluate(rfPredictions))

// COMMAND ----------

display(rfPredictions)

// COMMAND ----------

// MAGIC %md
// MAGIC So what exactly did `PolynomialExpansion` do?

// COMMAND ----------

// MAGIC %md
// MAGIC All interactions
// MAGIC  
// MAGIC \\[ \begin{bmatrix} a \times a & b \times a & c \times a & d \times a \\\ a \times b & b \times b & c \times b & d \times b \\\ a \times c & b \times c & c \times c & d \times c \\\ a \times d & b \times d & c \times d & d \times d \end{bmatrix}  \\]
// MAGIC  
// MAGIC Remove duplicates
// MAGIC  
// MAGIC \\[ \begin{bmatrix} a \times a \\\ a \times b & b \times b \\\ a \times c & b \times c & c \times c \\\ a \times d & b \times d & c \times d & d \times d \end{bmatrix}  \\]
// MAGIC  
// MAGIC Plus the original features
// MAGIC  
// MAGIC \\[ \begin{bmatrix} a & b & c & d \end{bmatrix} \\]

// COMMAND ----------

// MAGIC %md
// MAGIC Can we do better?  Let's build a grid of params and search using `CrossValidator`.

// COMMAND ----------

import org.apache.spark.ml.classification.RandomForestClassificationModel
 
import org.apache.spark.ml.param.ParamPair
 
val paramGridRand = new ParamGridBuilder()
  .addGrid(rf.maxDepth, Array(2, 4, 8, 12))
  .baseOn(new ParamPair(rf.numTrees, 20))
  .build()
 
val cvRand = new CrossValidator()
  .setEstimator(rfPipeline)
  .setEvaluator(multiEval)
  .setEstimatorParamMaps(paramGridRand)
  .setNumFolds(2)
 
val cvModelRand = cvRand.fit(irisTrain)
 
// avgMetrics is the metric average across the folds for the given params
val avgMetrics = cvModelRand.avgMetrics
paramGridRand.zip(avgMetrics).foreach(println)
println("\n\n\n")

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, let's view the resulting model.

// COMMAND ----------

println(cvModelRand.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[RandomForestClassificationModel].toDebugString)
