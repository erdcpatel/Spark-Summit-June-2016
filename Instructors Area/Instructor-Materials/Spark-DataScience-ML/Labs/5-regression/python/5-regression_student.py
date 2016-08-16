# Databricks notebook source exported at Mon, 15 Feb 2016 02:45:37 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Regression
# MAGIC  
# MAGIC This lab covers building regression models using linear regression and decision trees.  Also covered are regression metrics, bootstrapping, and some traditional model evaluation methods.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read in and prepare the data
# MAGIC  
# MAGIC First, we'll load the data from our parquet file.

# COMMAND ----------

baseDir = '/mnt/ml-class/'
irisDense = sqlContext.read.parquet(baseDir + 'irisDense.parquet').cache()

display(irisDense)

# COMMAND ----------

# MAGIC %md
# MAGIC View the dataset.

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
# MAGIC Prepare the data so that we have the sepal width as our target and a dense vector containing sepal length as our features.

# COMMAND ----------

from pyspark.sql.functions import udf, lit
from pyspark.sql.types import DoubleType
from pyspark.mllib.linalg import VectorUDT, Vectors

getElement = udf(lambda v, i: float(v[i]), DoubleType())
getElementAsVector = udf(lambda v, i: Vectors.dense([v[i]]), VectorUDT())

irisSepal = irisDense.select(getElement('features', lit(1)).alias('sepalWidth'),
                             getElementAsVector('features', lit(0)).alias('features'))
irisSepal.cache()

display(irisSepal)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build a linear regression model

# COMMAND ----------

# MAGIC %md
# MAGIC First, we'll sample from our dataset to obtain a [bootstrap sample](https://en.wikipedia.org/wiki/Bootstrapping_%28statistics%29) of our data.
# MAGIC  
# MAGIC When using a `DataFrame` we can call `.sample` to return a random sample with or without replacement.  `sample` takes in a boolean for whether to sample with replacement and a fraction for what percentage of the dataset to sample.  Note that if replacement is true we can sample more than 100% of the data.  For example, to choose approximately twice as much data as the original dataset we can set fraction equal to 2.0.
# MAGIC  
# MAGIC An explanation of `sample` can be found under `DataFrame` in both the [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample) and [Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrame) APIs.

# COMMAND ----------

help(irisSepal.sample)

# COMMAND ----------

irisSepalSample = irisSepal.sample(True, 1.0, 1)
display(irisSepalSample)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, let's create our linear regression object.

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr = (LinearRegression()
      .setLabelCol('sepalWidth')
      .setMaxIter(1000))
print lr.explainParams()

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll create a `Pipeline` that only contains one stage for the linear regression.

# COMMAND ----------

from pyspark.ml.pipeline import Pipeline
pipeline = Pipeline().setStages([lr])

pipelineModel = pipeline.fit(irisSepalSample)
sepalPredictions = pipelineModel.transform(irisSepalSample)

display(sepalPredictions)

# COMMAND ----------

# MAGIC %md
# MAGIC What does our resulting model look like?

# COMMAND ----------

lrModel = pipelineModel.stages[-1]
print type(lrModel)

print '\n', lrModel.intercept, lrModel.weights

print '\nsepalWidth = {0:.3f} + ({1:.3f} * sepalLength)'.format(lrModel.intercept, lrModel.weights[0])

# COMMAND ----------

# MAGIC %md
# MAGIC Let's visualize this model.

# COMMAND ----------

colorMap = 'Set1'  # was 'Set2', 'Set1', 'Dark2', 'winter'

def generatePlotDataAndModel(lrModels, linestyle='--', alpha=.80, x=x1, y=y1,
                             xlab='Sepal Length', ylab='Sepal Width'):
    fig, ax = prepareSubplot(np.arange(-1, 1.1, .2), np.arange(-1, 1.1, .2), figsize=(7., 5.))

    ax.set_xlim((-1.1, 1.1)), ax.set_ylim((-1.1, 1.1))
    ax.scatter(x, y, s=14**2, c='red', edgecolors='#444444', alpha=0.60, cmap=colorMap)
    ax.set_xlabel(xlab), ax.set_ylabel(ylab)

    for lrModel in lrModels:
        intercept = lrModel.intercept
        slope = lrModel.weights[0]
        lineStart = (-2.0, intercept + (slope * -2.0))
        lineEnd = (2.0, intercept + (slope * 2.0))
        line = [lineStart, lineEnd]
        xs, ys = zip(*line)

        ax.plot(xs, ys, c='orange', linestyle=linestyle, linewidth=3.0, alpha=alpha)

    fig.tight_layout()

    return fig

fig = generatePlotDataAndModel([lrModel])
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Boostrap sampling 100 models
# MAGIC  
# MAGIC In order to reason about how stable our models are and whether or not the coefficients are significantly different from zero, we'll draw 100 samples with replacement and generate a linear model for each of those samples.

# COMMAND ----------

def generateModels(df, pipeline, numModels=100):
    models = []

    for i in range(numModels):
        sample = df.sample(True, 1.0, i)
        pipelineModel = pipeline.fit(sample)
        models.append(pipelineModel.stages[-1])

    return models

sepalModels = generateModels(irisSepal, pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll convert our models to a `DataFrame` so we can analyze the different values we obtained for intercept and weight.

# COMMAND ----------

from pyspark.sql import Row
sepalModelsRow = map(lambda m: Row(intercept=float(m.intercept), weight=float(m.weights[0])), sepalModels)
sepalModelResults = sqlContext.createDataFrame(sepalModelsRow)
display(sepalModelResults)

# COMMAND ----------

# MAGIC %md
# MAGIC Then we can use `describe` to see the count, mean, and standard deviation of our intercept and weight.  Based on these results it is pretty clear that there isn't a significant relationship between sepal length and sepal width.

# COMMAND ----------

display(sepalModelResults.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Let's visualize the 100 models we just generated.

# COMMAND ----------

fig = generatePlotDataAndModel(sepalModels, linestyle='-', alpha=.05)
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll visualize the probability distribution function (PDF) for the weight.

# COMMAND ----------

from scipy import stats

w = sorted(map(lambda m: m.weights[0], sepalModels))
w2_5= (w[1] + w[2]) / 2.0
w97_5 = (w[96] + w[97]) / 2.0
wDiff = w97_5 - w2_5

density = stats.kde.gaussian_kde(w)
x = np.arange(-1.0, 1.0, .01)
xDensity = density(x)

dMin, dMax = xDensity.min(), xDensity.max()

fig, ax = prepareSubplot(np.arange(-.6, .7, .1), np.arange(-1, 10.1, 1.), figsize=(7., 5.))

circle1=plt.Rectangle((w2_5, 0), wDiff, 100, color='#444444', alpha=.10, linewidth=2)
ax.add_artist(circle1)

ax.plot(x, xDensity, color='red', linewidth=4.0, alpha=.70)
ax.plot([0, 0], [0, 100], color='orange', linestyle='--', linewidth=5.0)

ax.set_xlim((-.5, .4)), ax.set_ylim((-.3, 7.6))
ax.set_xlabel('weight'), ax.set_ylabel('density')
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC And we can also visualize the cumulative distribution function (CDF) for the weight.

# COMMAND ----------

def prepareCDFPlot(w):
    wMax, wMin = max(w), min(w)
    diff = wMax - wMin
    diffInc = round(diff / 10.0, 2)

    fig, ax = prepareSubplot(np.arange(round(wMin, 2), wMax + diffInc, diffInc), np.arange(0, 101, 10),
                             figsize=(7., 5.))

    x = list(range(len(w)))
    ax.plot(w, x, color='red', linewidth=4.0, alpha=.70)

    circle1=plt.Rectangle((wMin, 2.5), diff, 95.0, color='#444444', alpha=.10, linewidth=2)
    ax.add_artist(circle1)
    ax.plot([0, 0], [0, 100], color='orange', linestyle='--', linewidth=5.0)

    ax.set_xlim(wMin, wMax), ax.set_ylim(0, 100)
    ax.set_xlabel('weight'), ax.set_ylabel('percentile')
    return fig

fig = prepareCDFPlot(w)
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Petal width vs petal length
# MAGIC  
# MAGIC We saw that there wasn't a significant relationship between sepal width and sepal length.  Let's repeat the analysis for the petal attributes.

# COMMAND ----------

irisPetal = irisDense.select(getElement('features', lit(3)).alias('petalWidth'),
                             getElementAsVector('features', lit(2)).alias('features'))
irisPetal.cache()
display(irisPetal)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the linear regression estimator and pipeline estimator.

# COMMAND ----------

lrPetal = (LinearRegression()
           .setLabelCol('petalWidth'))

petalPipeline = Pipeline().setStages([lrPetal])

# COMMAND ----------

# MAGIC %md
# MAGIC Generate the models.

# COMMAND ----------

petalModels = generateModels(irisPetal, petalPipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll repeat the conversion of the model data to a `DataFrame` and then view the statistics on the `DataFrame`.

# COMMAND ----------

petalModelsRow = map(lambda m: Row(intercept=float(m.intercept), weight=float(m.weights[0])), petalModels)
petalModelResults = sqlContext.createDataFrame(petalModelsRow)
display(petalModelResults)

# COMMAND ----------

# MAGIC %md
# MAGIC From these results, we can clearly see that this weight is significantly different from zero.

# COMMAND ----------

display(petalModelResults.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Let's visualize the 100 models we just generated.

# COMMAND ----------

fig = generatePlotDataAndModel(petalModels, linestyle='-', alpha=.05, x=x2, y=y2,
                               xlab='Petal Length', ylab='Petal Width')
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll visualize the probability distribution function (PDF) for the weight.

# COMMAND ----------

w = sorted(map(lambda m: m.weights[0], petalModels))

density = stats.kde.gaussian_kde(w)
x = np.arange(.8, 1.2, .01)
xDensity = density(x)

dMin, dMax = xDensity.min(), xDensity.max()

w2_5= (w[1] + w[2]) / 2.0
w97_5 = (w[96] + w[97]) / 2.0
wDiff = w97_5 - w2_5

fig, ax = prepareSubplot(np.arange(.9, 1.2, .05), np.arange(-1, 25, 3.), figsize=(7., 5.))

circle1=plt.Rectangle((w2_5, 0), wDiff, 100, color='#444444', alpha=.10, linewidth=2)
ax.add_artist(circle1)

ax.plot(x, xDensity, color='red', linewidth=4.0, alpha=.70)
ax.plot([0, 0], [0, 100], color='orange', linestyle='--', linewidth=5.0)

ax.set_xlim((.9, 1.15)), ax.set_ylim((-.3, 25))
ax.set_xlabel('weight'), ax.set_ylabel('density')
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC And we can also visualize the cumulative distribution function (CDF) for the weight.

# COMMAND ----------

fig = prepareCDFPlot(w)
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC #### View and evaluate predictions

# COMMAND ----------

# MAGIC %md
# MAGIC To start, we'll generate the predictions by using the first model in `petalModels`.

# COMMAND ----------

petalPredictions = petalModels[0].transform(irisPetal)
display(petalPredictions)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll evaluate the model using the `RegressionEvaluator`.

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
regEval = RegressionEvaluator().setLabelCol('petalWidth')

print regEval.explainParams()

# COMMAND ----------

# MAGIC %md
# MAGIC The default value for `RegressionEvaluator` is root mean square error (RMSE).  Let's view that first.

# COMMAND ----------

print regEval.evaluate(petalPredictions)

# COMMAND ----------

# MAGIC %md
# MAGIC `RegressionEvaluator` also supports mean square error (MSE), \\( r^2 \\), and mean absolute error (MAE).  We'll view the \\( r^2 \\) metric next.

# COMMAND ----------

print regEval.evaluate(petalPredictions, {regEval.metricName: 'r2'})

# COMMAND ----------

# MAGIC %md
# MAGIC Let's evaluate our model on the sepal data as well.

# COMMAND ----------

sepalPredictions = sepalModels[0].transform(irisSepal)
print regEval.evaluate(sepalPredictions,
                       {regEval.metricName: 'r2', regEval.labelCol: 'sepalWidth'})
print regEval.evaluate(sepalPredictions,
                       {regEval.metricName: 'rmse', regEval.labelCol: 'sepalWidth'})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Regression with decision trees

# COMMAND ----------

from pyspark.ml.regression import DecisionTreeRegressor

dtr = DecisionTreeRegressor().setLabelCol('petalWidth')
print dtr.explainParams()

# COMMAND ----------

dtrModel = dtr.fit(irisPetal)
dtrPredictions = dtrModel.transform(irisPetal)
print regEval.evaluate(dtrPredictions, {regEval.metricName: 'r2'})
print regEval.evaluate(dtrPredictions, {regEval.metricName: 'rmse'})

# COMMAND ----------

# MAGIC %md
# MAGIC Let's also build a gradient boosted tree.

# COMMAND ----------

from pyspark.ml.regression import GBTRegressor
gbt = GBTRegressor().setLabelCol('petalWidth')
print gbt.explainParams()

# COMMAND ----------

gbtModel = gbt.fit(irisPetal)
gbtPredictions = gbtModel.transform(irisPetal)
print regEval.evaluate(gbtPredictions, {regEval.metricName: 'r2'})
print regEval.evaluate(gbtPredictions, {regEval.metricName: 'rmse'})

# COMMAND ----------

# MAGIC %md
# MAGIC We should really test our gradient boosted tree out-of-sample as it is easy to overfit with a GBT model.

# COMMAND ----------
