# Databricks notebook source exported at Mon, 15 Feb 2016 02:08:26 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------
# MAGIC %run /Users/admin@databricks.com/Labs/2-etl-kmeans/python/2-etl-kmeans_part1_answers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2

# COMMAND ----------

repr(irisTwoFeatures.first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC Let's view our `irisTwoFeatures` `DataFrame`.

# COMMAND ----------

irisTwoFeatures.take(5)

# COMMAND ----------

display(irisTwoFeatures)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Saving our DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC We'll be using parquet files to save our data.  More information about the parquet file format can be found on [parquet.apache.org](https://parquet.apache.org/documentation/latest/).

# COMMAND ----------

help(irisTwoFeatures.write.parquet)

# COMMAND ----------

import uuid
if 'parqUUID' not in locals():
    parqUUID = uuid.uuid1()
irisTwoFeatures.write.mode('overwrite').parquet('/tmp/{0}/irisTwoFeatures.parquet'.format(parqUUID))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we'll get a part file for each partition and that these files are compressed.

# COMMAND ----------

#display(dbutils.fs.ls(baseDir + 'irisTwoFeatures.parquet'))
display(dbutils.fs.ls('/tmp/{0}/irisTwoFeatures.parquet'.format(parqUUID)))

# COMMAND ----------

irisDFZeroIndex.write.mode('overwrite').parquet('/tmp/{0}/irisFourFeatures.parquet'.format(parqUUID))

# COMMAND ----------

# MAGIC %md
# MAGIC #### K-Means

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll build a k-means model using our two features and inspect the class hierarchy.
# MAGIC  
# MAGIC We'll build the k-means model using `KMeans`, an `ml` `Estimator`.  Details can be found in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.clustering) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.clustering.package) APIs.  Also, examples that use [PCA](http://spark.apache.org/docs/latest/ml-features.html#pca) and  [logistic regression](http://spark.apache.org/docs/latest/ml-guide.html#example-estimator-transformer-and-param) can be found in the ML Programming Guide.
# MAGIC  
# MAGIC Make sure to work with the `irisTwoFeatures` `DataFrame`.

# COMMAND ----------

# ANSWER
from pyspark.ml.clustering import KMeans

# Create a KMeans Estimator and set k=3, seed=5, maxIter=20, initSteps=1
kmeans = (KMeans()
          .setK(3)
          .setSeed(5)
          .setMaxIter(20)
          .setInitSteps(1))

#  Call fit on the estimator and pass in our DataFrame
model = kmeans.fit(irisTwoFeatures)

# Obtain the clusterCenters from the KMeansModel
centers = model.clusterCenters()

# Use the model to transform the DataFrame by adding cluster predictions
transformed = model.transform(irisTwoFeatures)

print centers

# COMMAND ----------

# TEST
import numpy as np
Test.assertTrue(np.allclose([ 0.35115296, -0.10691828], centers[0]),
                'incorrect centers.  check your params.')
Test.assertEquals(transformed.select('prediction').map(lambda r: r[0]).take(4), [1,1,1,1],
                  'incorrect predictions')
