// Databricks notebook source exported at Mon, 15 Feb 2016 02:45:37 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Regression
// MAGIC  
// MAGIC This lab covers building regression models using linear regression and decision trees.  Also covered are regression metrics, bootstrapping, and some traditional model evaluation methods.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Read in and prepare the data
// MAGIC  
// MAGIC First, we'll load the data from our parquet file.

// COMMAND ----------

val baseDir = "/mnt/ml-class/"
val irisDense = sqlContext.read.parquet(baseDir + "irisDense.parquet").cache
 
display(irisDense)

// COMMAND ----------

// MAGIC %md
// MAGIC View the dataset.

// COMMAND ----------

// MAGIC %md
// MAGIC Prepare the data so that we have the sepal width as our target and a dense vector containing sepal length as our features.

// COMMAND ----------

import org.apache.spark.sql.functions.{udf, lit}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
 
val getElement = udf { (v:Vector, i: Int) => v(i) }
val getElementAsVector = udf { (v:Vector, i: Int) => Vectors.dense(Array(v(i))) }
 
val irisSepal = irisDense.select(getElement($"features", lit(1)).as("sepalWidth"),
                                 getElementAsVector($"features", lit(0)).as("features"))
irisSepal.cache()
 
display(irisSepal)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Build a linear regression model

// COMMAND ----------

// MAGIC %md
// MAGIC First, we'll sample from our dataset to obtain a [bootstrap sample](https://en.wikipedia.org/wiki/Bootstrapping_%28statistics%29) of our data.
// MAGIC  
// MAGIC When using a `DataFrame` we can call `.sample` to return a random sample with or without replacement.  `sample` takes in a boolean for whether to sample with replacement and a fraction for what percentage of the dataset to sample.  Note that if replacement is true we can sample more than 100% of the data.  For example, to choose approximately twice as much data as the original dataset we can set fraction equal to 2.0.
// MAGIC  
// MAGIC An explanation of `sample` can be found under `DataFrame` in both the [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample) and [Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrame) APIs.

// COMMAND ----------

val irisSepalSample = irisSepal.sample(true, 1.0, 1)
display(irisSepalSample)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's create our linear regression object.

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression
 
val lr = new LinearRegression()
  .setLabelCol("sepalWidth")
  .setMaxIter(1000)
println(lr.explainParams)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll create a `Pipeline` that only contains one stage for the linear regression.

// COMMAND ----------

import org.apache.spark.ml.Pipeline
 
val pipeline = new Pipeline().setStages(Array(lr))
 
val pipelineModel = pipeline.fit(irisSepalSample)
val sepalPredictions = pipelineModel.transform(irisSepalSample)
 
display(sepalPredictions)

// COMMAND ----------

// MAGIC %md
// MAGIC What does our resulting model look like?

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegressionModel
 
val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
println(lrModel.getClass)
 
println(s"\n${lrModel.intercept} ${lrModel.weights}")
 
println(f"\nsepalWidth = ${lrModel.intercept}%.3f + (${lrModel.weights(0)}%.3f * sepalLength)")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Boostrap sampling 100 models
// MAGIC  
// MAGIC In order to reason about how stable our models are and whether or not the coefficients are significantly different from zero, we'll draw 100 samples with replacement and generate a linear model for each of those samples.

// COMMAND ----------

import org.apache.spark.sql.DataFrame
 
def generateModels(df: DataFrame, pipeline: Pipeline, numModels: Int = 100) = {
  (0 until numModels).map(i => {
    val sample = df.sample(true, 1.0, i)
    val pipelineModel = pipeline.fit(sample)
    pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
  })
}
 
val sepalModels = generateModels(irisSepal, pipeline)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll convert our models to a `DataFrame` so we can analyze the different values we obtained for intercept and weight.

// COMMAND ----------

val sepalModelsTuple = sepalModels.map(m => (m.intercept, m.weights(0)))
val sepalModelResults = sqlContext.createDataFrame(sepalModelsTuple)
  .toDF("intercept", "weight")
display(sepalModelResults)

// COMMAND ----------

// MAGIC %md
// MAGIC Then we can use `describe` to see the count, mean, and standard deviation of our intercept and weight.  Based on these results it is pretty clear that there isn't a significant relationship between sepal length and sepal width.

// COMMAND ----------

display(sepalModelResults.describe())

// COMMAND ----------

// MAGIC %md
// MAGIC #### Petal width vs petal length
// MAGIC  
// MAGIC We saw that there wasn't a significant relationship between sepal width and sepal length.  Let's repeat the analysis for the petal attributes.

// COMMAND ----------

val irisPetal = irisDense.select(getElement($"features", lit(3)).as("petalWidth"),
                                 getElementAsVector($"features", lit(2)).as("features"))
irisPetal.cache()
display(irisPetal)

// COMMAND ----------

// MAGIC %md
// MAGIC Create the linear regression estimator and pipeline estimator.

// COMMAND ----------

val lrPetal = new LinearRegression()
  .setLabelCol("petalWidth")
 
val petalPipeline = new Pipeline().setStages(Array(lrPetal))

// COMMAND ----------

// MAGIC %md
// MAGIC Generate the models.

// COMMAND ----------

val petalModels = generateModels(irisPetal, petalPipeline)

// COMMAND ----------

// MAGIC %md
// MAGIC We'll repeat the conversion of the model data to a `DataFrame` and then view the statistics on the `DataFrame`.

// COMMAND ----------

val petalModelsTuple = petalModels.map(m => (m.intercept, m.weights(0)))
val petalModelResults = sqlContext.createDataFrame(petalModelsTuple)
  .toDF("intercept", "weight")
display(petalModelResults)

// COMMAND ----------

// MAGIC %md
// MAGIC From these results, we can clearly see that this weight is significantly different from zero.

// COMMAND ----------

display(petalModelResults.describe())

// COMMAND ----------

// MAGIC %md
// MAGIC #### View and evaluate predictions

// COMMAND ----------

// MAGIC %md
// MAGIC To start, we'll generate the predictions by using the first model in `petalModels`.

// COMMAND ----------

val petalPredictions = petalModels(0).transform(irisPetal)
display(petalPredictions)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll evaluate the model using the `RegressionEvaluator`.

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator
val regEval = new RegressionEvaluator().setLabelCol("petalWidth")
 
println(regEval.explainParams)

// COMMAND ----------

// MAGIC %md
// MAGIC The default value for `RegressionEvaluator` is root mean square error (RMSE).  Let's view that first.

// COMMAND ----------

println(regEval.evaluate(petalPredictions))

// COMMAND ----------

// MAGIC %md
// MAGIC `RegressionEvaluator` also supports mean square error (MSE), \\( r^2 \\), and mean absolute error (MAE).  We'll view the \\( r^2 \\) metric next.

// COMMAND ----------

import org.apache.spark.ml.param.ParamMap
 
println(regEval.evaluate(petalPredictions, ParamMap(regEval.metricName -> "r2")))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's evaluate our model on the sepal data as well.

// COMMAND ----------

val sepalPredictions = sepalModels(0).transform(irisSepal)
println(regEval.evaluate(sepalPredictions,
                         ParamMap(regEval.metricName -> "r2", regEval.labelCol -> "sepalWidth")))
println(regEval.evaluate(sepalPredictions,
                         ParamMap(regEval.metricName -> "rmse", regEval.labelCol -> "sepalWidth")))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Regression with decision trees

// COMMAND ----------

import org.apache.spark.ml.regression.DecisionTreeRegressor
 
val dtr = new DecisionTreeRegressor().setLabelCol("petalWidth")
println(dtr.explainParams)

// COMMAND ----------

val dtrModel = dtr.fit(irisPetal)
val dtrPredictions = dtrModel.transform(irisPetal)
println(regEval.evaluate(dtrPredictions, ParamMap(regEval.metricName -> "r2")))
println(regEval.evaluate(dtrPredictions, ParamMap(regEval.metricName -> "rmse")))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's also build a gradient boosted tree.

// COMMAND ----------

import org.apache.spark.ml.regression.GBTRegressor
val gbt = new GBTRegressor().setLabelCol("petalWidth")
println(gbt.explainParams)

// COMMAND ----------

val gbtModel = gbt.fit(irisPetal)
val gbtPredictions = gbtModel.transform(irisPetal)
println(regEval.evaluate(gbtPredictions, ParamMap(regEval.metricName -> "r2")))
println(regEval.evaluate(gbtPredictions, ParamMap(regEval.metricName -> "rmse")))

// COMMAND ----------

// MAGIC %md
// MAGIC We should really test our gradient boosted tree out-of-sample as it is easy to overfit with a GBT model.
