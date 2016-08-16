# Databricks notebook source exported at Wed, 10 Feb 2016 23:34:31 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------
# MAGIC %run /Users/admin@databricks.com/Labs/3-pipeline-logistic/python/3-pipeline-logistic_part4_answers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5

# COMMAND ----------

print pipelineModelLR.stages
print '\n{0}'.format(pipelineModelLR.stages[-1].weights)

# COMMAND ----------

# MAGIC %md
# MAGIC Leaving our features to range from 0 to 3 means that a value of 2 has twice the impact in our model than a value of 1.  Since these buckets were based on increasing numeric values this is not unreasonable; however, we might want to convert each of these values to a dummy feature that takes on either a 0 or 1 corresponding to whether the value occurs.  This allows the model to measure the impact of the occurrences of the individual values and allows for non-linear relationships.
# MAGIC  
# MAGIC To do this we'll use the `OneHotEncoder` estimator.

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder

oneHotLength = (OneHotEncoder()
                .setInputCol('lengthFeatures')
                .setOutputCol('lengthOneHot'))

pipeline.setStages([lengthBucketizer, widthBucketizer, oneHotLength])

irisWithOneHotLength = pipeline.fit(irisTrain).transform(irisTrain)
display(irisWithOneHotLength)

# COMMAND ----------

irisWithOneHotLength.select('lengthOneHot').first()

# COMMAND ----------

# MAGIC %md
# MAGIC Create a `OneHotEncoder` for width as well, and combine both encoders together into a `featuresBucketized` column.

# COMMAND ----------

oneHotWidth = (OneHotEncoder()
               .setInputCol('widthFeatures')
               .setOutputCol('widthOneHot'))

assembleOneHot = (VectorAssembler()
                  .setInputCols(['lengthOneHot', 'widthOneHot'])
                  .setOutputCol('featuresBucketized'))

pipeline.setStages([lengthBucketizer, widthBucketizer, oneHotLength, oneHotWidth, assembleOneHot])

display(pipeline.fit(irisTrain).transform(irisTrain))

# COMMAND ----------

# MAGIC %md
# MAGIC Create the full `Pipeline` through logistic regression and make predictions on the test data.

# COMMAND ----------

pipeline.setStages([lengthBucketizer, widthBucketizer, oneHotLength, oneHotWidth, assembleOneHot, lr])

pipelineModelLR2 = pipeline.fit(irisTrain)

irisTestPredictions2 = (pipelineModelLR2
                        .transform(irisTest)
                        .cache())
display(irisTestPredictions2)

# COMMAND ----------

# MAGIC %md
# MAGIC What does our new model look like?

# COMMAND ----------

logisticModel = pipelineModelLR2.stages[-1]
print logisticModel.intercept
print repr(logisticModel.weights)

# COMMAND ----------

# MAGIC %md
# MAGIC What about model accuracy?

# COMMAND ----------

from pyspark.sql.functions import col

def modelAccuracy(df):
  return (df
          .select((col('prediction') == col('label')).cast('int').alias('correct'))
          .groupBy()
          .avg('correct')
          .first()[0])

modelOneAccuracy = modelAccuracy(irisTestPredictions)
modelTwoAccuracy = modelAccuracy(irisTestPredictions2)

print 'modelOneAccuracy: {0:.3f}'.format(modelOneAccuracy)
print 'modelTwoAccuracy: {0:.3f}'.format(modelTwoAccuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC Or we can use SQL instead.

# COMMAND ----------

irisTestPredictions.registerTempTable('modelOnePredictions')
sqlResult = sqlContext.sql('select avg(int(prediction == label)) from modelOnePredictions')
display(sqlResult)

# COMMAND ----------

# MAGIC %md
# MAGIC An even better option is to use the tools already built-in to Spark.  The MLlib guide has a lot of information regarding [evaluation metrics](http://spark.apache.org/docs/latest/mllib-evaluation-metrics.html).  For ML, you can find details in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation) and [Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.package) APIs.
# MAGIC  
# MAGIC A common metric used for logistic regression is area under the ROC curve (AUC).  We can use the `BinaryClasssificationEvaluator` to obtain the AUC for our two models.  Make sure to set the metric to "areaUnderROC" and that you set the rawPrediction column to "rawPrediction".
# MAGIC  
# MAGIC Recall that `irisTestPredictions` are the test predictions from our first model and `irisTestPredictions2` are the test predictions from our second model.

# COMMAND ----------

# TODO: Replace <FILL IN> with appropriate code
from pyspark.ml.evaluation import BinaryClassificationEvaluator

binaryEvaluator = (<FILL IN>
                   .<FILL IN>
                   .<FILL IN>)

firstModelTestAUC = binaryEvaluator.evaluate(irisTestPredictions)
secondModelTestAUC = binaryEvaluator.evaluate(irisTestPredictions2)

print 'First model AUC: {0:.3f}'.format(firstModelTestAUC)
print 'Second model AUC: {0:.3f}'.format(secondModelTestAUC)

irisTrainPredictions = pipelineModelLR.transform(irisTrain)
irisTrainPredictions2 = pipelineModelLR2.transform(irisTrain)

firstModelTrainAUC = binaryEvaluator.evaluate(irisTrainPredictions)
secondModelTrainAUC = binaryEvaluator.evaluate(irisTrainPredictions2)

print '\nFirst model training AUC: {0:.3f}'.format(firstModelTrainAUC)
print 'Second model training AUC: {0:.3f}'.format(secondModelTrainAUC)

# COMMAND ----------

# TEST
Test.assertTrue(firstModelTestAUC > .95, 'incorrect firstModelTestAUC')
Test.assertTrue(secondModelTrainAUC > .95, 'incorrect secondModelTrainAUC')

# COMMAND ----------

# MAGIC %md
# MAGIC **Visualization: ROC curve **
# MAGIC  
# MAGIC We will now visualize how well the model predicts our target.  To do this we generate a plot of the ROC curve.  The ROC curve shows us the trade-off between the false positive rate and true positive rate, as we liberalize the threshold required to predict a positive outcome.  A random model is represented by the dashed line.

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

def prepareSubplot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999',
                gridWidth=1.0, subplots=(1, 1)):
    """Template for generating the plot layout."""
    plt.close()
    fig, axList = plt.subplots(subplots[0], subplots[1], figsize=figsize, facecolor='white',
                               edgecolor='white')
    if not isinstance(axList, np.ndarray):
        axList = np.array([axList])

    for ax in axList.flatten():
        ax.axes.tick_params(labelcolor='#999999', labelsize='10')
        for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
            axis.set_ticks_position('none')
            axis.set_ticks(ticks)
            axis.label.set_color('#999999')
            if hideLabels: axis.set_ticklabels([])
        ax.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
        map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])

    if axList.size == 1:
        axList = axList[0]  # Just return a single axes object for a regular plot
    return fig, axList

# COMMAND ----------

def generateROC(ax, labelsAndScores):
    labelsAndWeights = labelsAndScores.collect()
    labelsAndWeights.sort(key=lambda (k, v): v, reverse=True)
    labelsByWeight = np.array([0.0] + [k for (k, v) in labelsAndWeights])

    length = labelsByWeight.size - 1
    truePositives = labelsByWeight.cumsum()
    numPositive = truePositives[-1]
    falsePositives = np.arange(0.0, length + 1, 1.) - truePositives

    truePositiveRate = truePositives / numPositive
    falsePositiveRate = falsePositives / (length - numPositive)

    # Generate layout and plot data
    ax.set_xlim(-.05, 1.05), ax.set_ylim(-.05, 1.05)
    ax.set_ylabel('True Positive Rate (Sensitivity)')
    ax.set_xlabel('False Positive Rate (1 - Specificity)')
    ax.plot(falsePositiveRate, truePositiveRate, color='red', linestyle='-', linewidth=3.)
    ax.plot((0., 1.), (0., 1.), linestyle='--', color='orange', linewidth=2.)  # Baseline model


labelsAndScores = (irisTestPredictions
                   .select('label', 'rawPrediction')
                   .rdd
                   .map(lambda r: (r[0], r[1][1])))

labelsAndScores2 = (irisTestPredictions2
                    .select('label', 'rawPrediction')
                    .rdd
                    .map(lambda r: (r[0], r[1][1])))

fig, axList = prepareSubplot(np.arange(0., 1.1, 0.1), np.arange(0., 1.1, 0.1), figsize=(12., 5.), subplots=(1,2))
ax0, ax1 = axList
ax0.set_title('First Model', color='#999999')
ax1.set_title('Second Model', color='#999999')
generateROC(axList[0], labelsAndScores)
generateROC(axList[1], labelsAndScores2)
display(fig)

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

metric = 'precision'

multiclassEval = MulticlassClassificationEvaluator()

multiclassEval.setMetricName(metric)
print 'Model one {0}: {1:.3f}'.format(metric, multiclassEval.evaluate(irisTestPredictions))
print 'Model two {0}: {1:.3f}\n'.format(metric, multiclassEval.evaluate(irisTestPredictions2))

# COMMAND ----------

import inspect
print inspect.getsource(MulticlassClassificationEvaluator)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using MLlib instead of ML
# MAGIC  
# MAGIC We've been using `ml` transformers, estimators, pipelines, and evaluators.  How can we accomplish the same things with MLlib?

# COMMAND ----------

irisTestPredictions.columns

# COMMAND ----------

irisTestPredictions.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull the data that we need from our `DataFrame` and create `BinaryClassificationMetrics`.

# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics

modelOnePredictionLabel = (irisTestPredictions
                           .select('rawPrediction', 'label')
                           .rdd
                           .map(lambda r: (float(r[0][1]), r[1])))

modelTwoPredictionLabel = (irisTestPredictions2
                           .select('rawPrediction', 'label')
                           .rdd
                           .map(lambda r: (float(r[0][1]), r[1])))

metricsOne = BinaryClassificationMetrics(modelOnePredictionLabel)
metricsTwo = BinaryClassificationMetrics(modelTwoPredictionLabel)

print metricsOne.areaUnderROC
print metricsTwo.areaUnderROC

# COMMAND ----------

# MAGIC %md
# MAGIC To build a logistic regression model with MLlib we'll need the data to be an RDD of `LabeledPoints`.  For testing purposes we'll pull out the label and features into a tuple, since we'll want to make predictions directly on the features and not on a `LabeledPoint`.

# COMMAND ----------

from pyspark.mllib.regression import LabeledPoint

irisTrainRDD = (irisTrainPredictions
                .select('label', 'featuresBucketized')
                .map(lambda r: LabeledPoint(r[0], r[1]))
                .cache())

irisTestRDD = (irisTestPredictions
               .select('label', 'featuresBucketized')
               .map(lambda r: (r[0], r[1]))
               .cache())

print irisTrainRDD.take(2)
print irisTestRDD.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we can use MLlib's logistic regression on our `RDD` of `LabeledPoints`.  Note that we'll use `LogisticRegressionWithLBFGS` as it tends to converge faster than `LogisticRegressionWithSGD`.

# COMMAND ----------

from pyspark.mllib.classification import LogisticRegressionWithLBFGS
help(LogisticRegressionWithLBFGS)

# COMMAND ----------

mllibModel = LogisticRegressionWithLBFGS.train(irisTrainRDD, iterations=1000, regParam=0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's calculate our accuracy using `RDDs`.

# COMMAND ----------

rddPredictions = mllibModel.predict(irisTestRDD.values())
predictAndLabels = rddPredictions.zip(irisTestRDD.keys())

mllibAccuracy = predictAndLabels.map(lambda (p, l): p == l).mean()
print 'MLlib model accuracy: {0:.3f}'.format(mllibAccuracy)


# COMMAND ----------
