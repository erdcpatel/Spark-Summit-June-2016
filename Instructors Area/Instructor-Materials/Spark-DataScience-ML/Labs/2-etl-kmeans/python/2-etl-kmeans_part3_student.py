# Databricks notebook source exported at Mon, 15 Feb 2016 02:08:26 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------
# MAGIC %run /Users/admin@databricks.com/Labs/2-etl-kmeans/python/2-etl-kmeans_part2_answers

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 3

# COMMAND ----------

# MAGIC %md
# MAGIC From the class hierarchy it is clear that `KMeans` is an `Estimator` while `KMeansModel` is a `Transformer`.

# COMMAND ----------

print '*** KMeans instance inheritance partial tree ***'
print '\n{0}\n'.format(type(kmeans))
print '\n'.join(map(str, type(kmeans).__bases__))
print '\n{0}'.format(type(kmeans).__bases__[0].__bases__)

print '\n\n*** KMeansModel instance inheritance partial tree ***'
print '\n{0}\n'.format(type(model))
print '\n'.join(map(str, type(model).__bases__)) + '\n'
print '\n'.join(map(str, type(model).__bases__[0].__bases__))
print '\n{0}'.format(type(model).__bases__[0].__bases__[0].__bases__)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's print the three centroids of our model

# COMMAND ----------

print centers

# COMMAND ----------

# MAGIC %md
# MAGIC Note that our predicted cluster is appended, as a column, to our input `DataFrame`.  Here it would be desirable to see consistency between label and prediction.  These don't need to be the same number but if label 0 is usually predicted to be cluster 1 that would indicate that our unsupervised learning is naturally grouping the data into species.

# COMMAND ----------

display(transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### K-Means Visualized

# COMMAND ----------

modelCenters = []
iterations = [0, 2, 4, 7, 10, 20]
for i in iterations:
    kmeans = KMeans(k=3, seed=5, maxIter=i, initSteps=1)
    model = kmeans.fit(irisTwoFeatures)
    modelCenters.append(model.clusterCenters())

# COMMAND ----------

print 'modelCenters:'
for centroids in modelCenters:
  print centroids

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

# MAGIC %md
# MAGIC Display the clustroid centers with the original labeled points and circles representing distance from the centroids.

# COMMAND ----------

data = irisTwoFeatures.collect()
features, labels = zip(*data)
x, y = zip(*features)

centroidX, centroidY = zip(*centers)
colorMap = 'Set1'  # was 'Set2', 'Set1', 'Dark2', 'winter'

fig, ax = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(8,6))
plt.scatter(x, y, s=14**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap=colorMap)
plt.scatter(centroidX, centroidY, s=22**2, marker='*', c='yellow')
cmap = cm.get_cmap(colorMap)

colorIndex = [.99, 0., .5]
for i, (x,y) in enumerate(centers):
    print cmap(colorIndex[i])
    for size in [.10, .20, .30, .40, .50]:
        circle1=plt.Circle((x,y),size,color=cmap(colorIndex[i]), alpha=.10, linewidth=2)
        ax.add_artist(circle1)

ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize how the clustroid centers move as the k-means algorithm iterates.

# COMMAND ----------

x, y = zip(*features)

oldCentroidX, oldCentroidY = None, None

fig, axList = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(11, 15),
                             subplots=(3, 2))
axList = axList.flatten()

for i,ax in enumerate(axList[:]):
    ax.set_title('K-means for {0} iterations'.format(iterations[i]), color='#999999')
    centroids = modelCenters[i]
    centroidX, centroidY = zip(*centroids)

    ax.scatter(x, y, s=10**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap=colorMap)
    ax.scatter(centroidX, centroidY, s=16**2, marker='*', c='yellow')
    if oldCentroidX and oldCentroidY:
      ax.scatter(oldCentroidX, oldCentroidY, s=16**2, marker='*', c='grey')
    cmap = cm.get_cmap(colorMap)

    colorIndex = [.99, 0., .5]
    for i, (x1,y1) in enumerate(centroids):
      print cmap(colorIndex[i])
      circle1=plt.Circle((x1,y1),.35,color=cmap(colorIndex[i]), alpha=.40)
      ax.add_artist(circle1)

    ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
    oldCentroidX, oldCentroidY = centroidX, centroidY
#axList[-1].cla()
#axList[-1].get_yaxis().set_ticklabels([])
#axList[-1].get_xaxis().set_ticklabels([])

plt.tight_layout()

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Summary plot of the centroid movement.

# COMMAND ----------

centroidX, centroidY = zip(*centers)

# generate layout and plot data
def plotKMeansTrack(x=x, y=y, labels=labels):
    fig, ax = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(8, 6))
    ax.set_ylim(-1, 1), ax.set_xlim(-1, 1)
    #plt.scatter(x, y, s=14**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap='winter')
    cmap = cm.get_cmap(colorMap)

    colorIndex = [.99, 0.0, .5]

    alphas = [.05, .10, .15, .20, .30, .40]
    sizes = [8, 12, 16, 20, 24, 28]

    for iteration, centroids in enumerate(modelCenters):
        centroidX, centroidY = zip(*centroids)
        color = 'lightgrey' if iteration < 5 else 'yellow'
        plt.scatter(centroidX, centroidY, s=sizes[iteration]**2, marker='*', c=color)

        for i, (x,y) in enumerate(centroids):
            print cmap(colorIndex[i])
            circle1=plt.Circle((x,y),.35,color=cmap(colorIndex[i%3]), alpha=alphas[iteration], linewidth=2.0)
            ax.add_artist(circle1)


    ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
    display(fig)

plotKMeansTrack(centroidX, centroidY)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using MLlib instead of ML

# COMMAND ----------

# MAGIC %md
# MAGIC First, convert our `DataFrame` into an `RDD`.

# COMMAND ----------

# Note that .rdd is not necessary, but is here to illustrate that we are working with an RDD
irisTwoFeaturesRDD = (irisTwoFeatures
                      .rdd
                      .map(lambda r: (r[1], r[0])))
irisTwoFeaturesRDD.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Then import MLlib's `KMeans` as `MLlibKMeans` to differentiate it from `ml.KMeans`

# COMMAND ----------

from pyspark.mllib.clustering import KMeans as MLlibKMeans

help(MLlibKMeans)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, let's build our k-means model.  Here are the relevant [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.clustering.KMeans) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.clustering.KMeans) APIs.
# MAGIC  
# MAGIC Make sure to set `k` to 3, `maxIterations` to 20, `seed` to 5, and `initializationSteps` to 1.  Also, note that we returned an `RDD` with (label, feature) tuples.  You'll just need the features, which you can obtain by calling `.values()` on `irisTwoFeaturesRDD`.

# COMMAND ----------

# TODO: Replace <FILL IN> with appropriate code
mllibKMeans = MLlibKMeans.<FILL IN>

print 'mllib: {0}'.format(mllibKMeans.clusterCenters)
print 'ml:    {0}'.format(centers)

# COMMAND ----------

# TEST
import numpy as np
Test.assertTrue(np.allclose(mllibKMeans.clusterCenters, centers), "Your mllib and ml models don't match")

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have an `mllibKMeans` model how do we generate predictions and compare those to our labels?

# COMMAND ----------

predictionsRDD = mllibKMeans.predict(irisTwoFeaturesRDD.values())
print predictionsRDD.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll use `zip` to combine the feature and prediction RDDs together.  Note that zip assumes that the RDDs have the same number of partitions and that each partition has the same number of elements.  This is true here as our predictions were the result of a `map` operation on the feature RDD.

# COMMAND ----------

combinedRDD = irisTwoFeaturesRDD.zip(predictionsRDD)
combinedRDD.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's compare this to the result from `ml`.

# COMMAND ----------

display(transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### How do the `ml` and `mllib` implementations differ?

# COMMAND ----------

import inspect
print inspect.getsource(kmeans.fit)

# COMMAND ----------

print inspect.getsource(kmeans._fit)

# COMMAND ----------

print inspect.getsource(kmeans._fit_java)

# COMMAND ----------

# MAGIC %md
# MAGIC The `ml` version of k-means is just a wrapper to MLlib's implementation.  Let's take a look here:
# MAGIC [org.apache.spark.ml.clustering.KMeans source](https://github.com/apache/spark/blob/e1e77b22b3b577909a12c3aa898eb53be02267fd/mllib/src/main/scala/org/apache/spark/ml/clustering/KMeans.scala#L192).
# MAGIC  
# MAGIC How is $ being used in this function? `Param` [source code](https://github.com/apache/spark/blob/2b574f52d7bf51b1fe2a73086a3735b633e9083f/mllib/src/main/scala/org/apache/spark/ml/param/params.scala#L643) has the answer.
# MAGIC  
# MAGIC Which is different than $'s usage for SQL columns where it is a [string interpolator that returns a ColumnName](https://github.com/apache/spark/blob/3d683a139b333456a6bd8801ac5f113d1ac3fd18/sql/core/src/main/scala/org/apache/spark/sql/SQLContext.scala#L386)

# COMMAND ----------
