# Databricks notebook source exported at Mon, 15 Feb 2016 02:40:09 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Decision Trees
# MAGIC  
# MAGIC This lab covers decision trees and random forests, while introducing metadata, cross-validation, `StringIndexer`, and `PolynomialExpansion`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare the data

# COMMAND ----------

# MAGIC %md
# MAGIC Load in the data from a parquet file.

# COMMAND ----------

baseDir = '/mnt/ml-class/'
irisFourFeatures = sqlContext.read.parquet(baseDir + 'irisFourFeatures.parquet')
print '\n'.join(map(repr, irisFourFeatures.take(2)))

# COMMAND ----------

# MAGIC %md
# MAGIC Convert the data from `SparseVector` to `DenseVector` types.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.mllib.linalg import Vectors, VectorUDT, DenseVector

sparseToDense = udf(lambda sv: Vectors.dense(sv.toArray()), VectorUDT())
irisDense = irisFourFeatures.select(sparseToDense('features').alias('features'), 'label')

print '\n'.join(map(repr, irisDense.take(2)))

# COMMAND ----------

# MAGIC %md
# MAGIC Save the new format for use in another notebook.

# COMMAND ----------

#irisDense.write.mode('overwrite').parquet('/tmp/irisDense.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize the data.

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

data = irisDense.collect()
features, labels = zip(*data)
x1, y1, x2, y2 = zip(*features)

colorMap = 'Set1'  # was 'Set2', 'Set1', 'Dark2', 'winter'

fig, axList = prepareSubplot(np.arange(-1, 1.1, .2), np.arange(-1, 1.1, .2), figsize=(11., 5.), subplots=(1, 2))
ax0, ax1 = axList

ax0.scatter(x1, y1, s=14**2, c=labels, edgecolors='#444444', alpha=0.80, cmap=colorMap)
ax0.set_xlabel('Sepal Length'), ax0.set_ylabel('Sepal Width')

ax1.scatter(x2, y2, s=14**2, c=labels, edgecolors='#444444', alpha=0.80, cmap=colorMap)
ax1.set_xlabel('Petal Length'), ax1.set_ylabel('Petal Width')

fig.tight_layout()

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Split the data into train and test sets and visualize the datasets.

# COMMAND ----------

irisTest, irisTrain = irisDense.randomSplit([.30, .70], seed=1)
irisTest.cache()
irisTrain.cache()

print 'Items in test datset: {0}'.format(irisTest.count())
print 'Items in train dataset: {0}'.format(irisTrain.count())

# COMMAND ----------

dataTrain = irisTrain.collect()
featuresTrain, labelsTrain = zip(*dataTrain)
x1Train, y1Train, x2Train, y2Train = zip(*featuresTrain)

dataTest = irisTest.collect()
featuresTest, labelsTest = zip(*dataTest)
x1Test, y1Test, x2Test, y2Test = zip(*featuresTest)

trainPlot1 = (x1Train, y1Train, labelsTrain, 'Train Data', 'Sepal Length', 'Sepal Width')
trainPlot2 = (x2Train, y2Train, labelsTrain, 'Train Data', 'Petal Length', 'Petal Width')
testPlot1 = (x1Test, y1Test, labelsTest, 'Test Data', 'Sepal Length', 'Sepal Width')
testPlot2 = (x2Test, y2Test, labelsTest, 'Test Data', 'Petal Length', 'Petal Width')
plotData = [trainPlot1, testPlot1, trainPlot2, testPlot2]

# COMMAND ----------

fig, axList = prepareSubplot(np.arange(-1, 1.1, .2), np.arange(-1, 1.1, .2), figsize=(11.,10.), subplots=(2, 2))

for ax, pd in zip(axList.flatten(), plotData):
    ax.scatter(pd[0], pd[1], s=14**2, c=pd[2], edgecolors='#444444', alpha=0.80, cmap=colorMap)
    ax.set_xlabel(pd[4]), ax.set_ylabel(pd[5])
    ax.set_title(pd[3], color='#999999')

    ax.set_xlim((-1.1, 1.1))
    ax.set_ylim((-1.1, 1.1))

fig.tight_layout()

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update the metadata for decision trees and build a tree

# COMMAND ----------

# MAGIC %md
# MAGIC We use `StringIndexer` on our labels in order to obtain a `DataFrame` that decision trees can work with.  Here are the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.StringIndexer) APIs for `StringIndexer`.
# MAGIC  
# MAGIC You'll need to set the input column to "label", the output column to "indexed", and fit and transform using the `irisTrain` `DataFrame`.

# COMMAND ----------

# ANSWER
from pyspark.ml.feature import StringIndexer

stringIndexer = (StringIndexer()
                 .setInputCol('label')
                 .setOutputCol('indexed'))

indexerModel = stringIndexer.fit(irisTrain)
irisTrainIndexed = indexerModel.transform(irisTrain)
display(irisTrainIndexed)

# COMMAND ----------

# TEST
from test_helper import Test
Test.assertEquals(irisTrainIndexed.select('indexed').take(50)[-1][0], 2.0, 'incorrect values in indexed column')
Test.assertTrue(irisTrainIndexed.schema.fields[2].metadata != {}, 'indexed should have metadata')

# COMMAND ----------

# MAGIC %md
# MAGIC We've updated the metadata for the field.  Now we know that the field takes on three values and is nominal.

# COMMAND ----------

print irisTrainIndexed.schema.fields[1].metadata
print irisTrainIndexed.schema.fields[2].metadata

# COMMAND ----------

# MAGIC %md
# MAGIC Let's build a decision tree to classify our data.

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
help(DecisionTreeClassifier)

# COMMAND ----------

dt = (DecisionTreeClassifier()
      .setLabelCol('indexed')
      .setMaxDepth(5)
      .setMaxBins(10)
      .setImpurity('gini'))

# COMMAND ----------

print dt.explainParam('impurity')
print '\n', dt.explainParam('maxBins')

# COMMAND ----------

# MAGIC %md
# MAGIC View all of the parameters to see if there is anything we'd like to update.

# COMMAND ----------

print dt.explainParams()

# COMMAND ----------

# MAGIC %md
# MAGIC Fit the model and display predictions on the test data.

# COMMAND ----------

dtModel = dt.fit(irisTrainIndexed)
predictionsTest = dtModel.transform(indexerModel.transform(irisTest))
display(predictionsTest)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll evaluate the results of the model.

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
multiEval = (MulticlassClassificationEvaluator()
             .setMetricName('precision')
             .setLabelCol('indexed'))

print multiEval.evaluate(predictionsTest)

# COMMAND ----------

# MAGIC %md
# MAGIC View the decision tree model.

# COMMAND ----------

dtModelString = dtModel._java_obj.toDebugString()
print dtModelString

# COMMAND ----------

readableModel = dtModelString
for feature, name in enumerate(['Sepal Length', 'Sepal Width', 'Petal Length', 'Petal Width']):
    readableModel = readableModel.replace('feature {0}'.format(feature), name)

print readableModel

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross-validation

# COMMAND ----------

# MAGIC %md
# MAGIC Let's go ahead and find the best cross-validated model.  A `CrossValidator` requires an estimator to build the models, an evaluator to compare the performance of the models, a parameter grid that specifies which estimator parameters to tune, and the number of folds to use.
# MAGIC  
# MAGIC There is a good example in the [ML Guide](http://spark.apache.org/docs/latest/ml-guide.html#example-model-selection-via-cross-validation), although it is only in Scala.  The Python code is very similar.
# MAGIC  
# MAGIC The estimator that we will use is a `Pipeline` that has `stringIndexer` and `dt`.
# MAGIC  
# MAGIC The evaluator will be `multiEval`.  You just need to make sure the metric is "precision".
# MAGIC  
# MAGIC We'll use `ParamGridBuilder` to build a grid with `dt.maxDepth` values of 2, 4, 6, and 10 (in that order).
# MAGIC  
# MAGIC Finally, we'll use 5 folds.

# COMMAND ----------

# ANSWER
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.pipeline import Pipeline

cvPipeline = Pipeline().setStages([stringIndexer, dt])

multiEval.setMetricName('precision')

paramGrid = (ParamGridBuilder()
             .addGrid(dt.maxDepth, [2, 4, 6, 10])
             .build())

cv = (CrossValidator()
      .setEstimator(cvPipeline)
      .setEvaluator(multiEval)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5))

cvModel = cv.fit(irisTrain)
predictions = cvModel.transform(irisTest)
print multiEval.evaluate(predictions)

# COMMAND ----------

# TEST
Test.assertEquals(round(multiEval.evaluate(predictions), 2), .98, 'incorrect predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC What was our best model?

# COMMAND ----------

bestDTModel = cvModel.bestModel.stages[-1]
print bestDTModel

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see more details on what parameters were used to build the best model.

# COMMAND ----------

print bestDTModel._java_obj.parent().explainParams()

# COMMAND ----------

print bestDTModel._java_obj.parent().getMaxDepth()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Random forest and `PolynomialExpansion`
# MAGIC  
# MAGIC Next, we'll build a random forest.  Since we only have a few features and random forests tend to work better with a lot of features, we'll expand our features using `PolynomialExpansion`.

# COMMAND ----------

from pyspark.ml.feature import PolynomialExpansion

px = (PolynomialExpansion()
      .setInputCol('features')
      .setOutputCol('polyFeatures'))

print px.explainParams()

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll use the `RandomForestClassifier` to build our random forest model.

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier

rf = (RandomForestClassifier()
      .setLabelCol('indexed')
      .setFeaturesCol('polyFeatures'))

print rf.explainParams()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's set some params based on what we just read.

# COMMAND ----------

(rf
 .setMaxBins(10)
 .setMaxDepth(2)
 .setNumTrees(20)
 .setSeed(0))

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll build a pipeline that includes the `StringIndexer`, `PolynomialExpansion`, and `RandomForestClassifier`.

# COMMAND ----------

rfPipeline = Pipeline().setStages([stringIndexer, px, rf])
rfModelPipeline = rfPipeline.fit(irisTrain)
rfPredictions = rfModelPipeline.transform(irisTest)

print multiEval.evaluate(rfPredictions)

# COMMAND ----------

display(rfPredictions)

# COMMAND ----------

# MAGIC %md
# MAGIC So what exactly did `PolynomialExpansion` do?

# COMMAND ----------

# MAGIC %md
# MAGIC All interactions
# MAGIC  
# MAGIC \\[ \begin{bmatrix} a \times a & b \times a & c \times a & d \times a \\\ a \times b & b \times b & c \times b & d \times b \\\ a \times c & b \times c & c \times c & d \times c \\\ a \times d & b \times d & c \times d & d \times d \end{bmatrix}  \\]
# MAGIC  
# MAGIC Remove duplicates
# MAGIC  
# MAGIC \\[ \begin{bmatrix} a \times a \\\ a \times b & b \times b \\\ a \times c & b \times c & c \times c \\\ a \times d & b \times d & c \times d & d \times d \end{bmatrix}  \\]
# MAGIC  
# MAGIC Plus the original features
# MAGIC  
# MAGIC \\[ \begin{bmatrix} a & b & c & d \end{bmatrix} \\]

# COMMAND ----------

# MAGIC %md
# MAGIC Can we do better?  Let's build a grid of params and search using `CrossValidator`.

# COMMAND ----------

paramGridRand = (ParamGridBuilder()
                 .addGrid(rf.maxDepth, [2, 4, 8, 12])
                 .baseOn({rf.numTrees, 20})
                 .build())

cvRand = (CrossValidator()
          .setEstimator(rfPipeline)
          .setEvaluator(multiEval)
          .setEstimatorParamMaps(paramGridRand)
          .setNumFolds(2))

cvModelRand = cvRand.fit(irisTrain)
predictionsRand = cvModelRand.transform(irisTest)
print multiEval.evaluate(predictionsRand)
print cvModelRand.bestModel.stages[-1]._java_obj.parent().getMaxDepth()

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, let's view the resulting model.

# COMMAND ----------

print cvModelRand.bestModel.stages[-1]._java_obj.toDebugString()

# COMMAND ----------
