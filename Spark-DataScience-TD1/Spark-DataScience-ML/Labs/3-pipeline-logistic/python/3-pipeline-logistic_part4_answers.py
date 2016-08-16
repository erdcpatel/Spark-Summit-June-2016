# Databricks notebook source exported at Wed, 10 Feb 2016 23:34:31 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------
# MAGIC %run /Users/admin@databricks.com/Labs/3-pipeline-logistic/python/3-pipeline-logistic_part3_answers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC #### Logistic Regression

# COMMAND ----------

# MAGIC %md
# MAGIC First let's look at our data by label.

# COMMAND ----------

display(irisSeparateFeatures.groupBy('label').count().orderBy('label'))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's build a model that tries to differentiate between the first two classes.

# COMMAND ----------

from pyspark.sql.functions import col
irisTwoClass = irisSeparateFeatures.filter(col('label') < 2)
display(irisTwoClass.groupBy('label').count().orderBy('label'))

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll split our dataset into test and train sets.

# COMMAND ----------

irisTest, irisTrain = irisTwoClass.randomSplit([.25, .75], seed=0)

# Cache as we'll be using these several times
irisTest.cache()
irisTrain.cache()

print 'Items in test datset: {0}'.format(irisTest.count())
print 'Items in train dataset: {0}'.format(irisTrain.count())

# COMMAND ----------

# MAGIC %md
# MAGIC And now let's build our logistic regression model.  LogisticRegression can be found in [pyspark.ml.classification](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression) for Python and the [org.apache.spark.ml.classification](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.classification.LogisticRegression) package for Scala.  The ML Guide also has a nice overview of [logistic regression](http://spark.apache.org/docs/latest/ml-classification-regression.html#logistic-regression).
# MAGIC  
# MAGIC Make sure to set the featuresCol to "featuresBucketized", the regParam to 0.0, the labelCol to "label", and the maxIter to 1000.
# MAGIC  
# MAGIC Also, set the pipeline stages to include the two bucketizers, assembler, and logistic regression.

# COMMAND ----------

# ANSWER
from pyspark.ml.classification import LogisticRegression

lr = (LogisticRegression()
      .setFeaturesCol('featuresBucketized')
      .setRegParam(0.0)
      .setLabelCol('label')
      .setMaxIter(1000))

pipeline.setStages([lengthBucketizer, widthBucketizer, assembler, lr])

pipelineModelLR = pipeline.fit(irisTrain)

irisTestPredictions = (pipelineModelLR
                       .transform(irisTest)
                       .cache())
display(irisTestPredictions)

# COMMAND ----------

irisTestPredictions.select("probability").first()[0][0]

# COMMAND ----------

# TEST
Test.assertTrue(sum(irisTestPredictions.select("probability").first()[0]) > .99,
                'incorrect build of the lr model')
