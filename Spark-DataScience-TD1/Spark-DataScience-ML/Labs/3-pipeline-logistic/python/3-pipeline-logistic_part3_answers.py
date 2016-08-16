# Databricks notebook source exported at Wed, 10 Feb 2016 23:34:31 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------
# MAGIC %run /Users/admin@databricks.com/Labs/3-pipeline-logistic/python/3-pipeline-logistic_part2_answers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3

# COMMAND ----------

# MAGIC %md
# MAGIC Let's just check and see that our norms are equal to 1.0

# COMMAND ----------

l2Norm = udf(lambda v: float(v.norm(2.0)), DoubleType())

featureLengths = irisNormalized.select(l2Norm('features').alias('featuresLength'),
                                       l2Norm('featureNorm').alias('featureNormLength'))
display(featureLengths)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, let's bucketize our features.  This will allow us to convert continuous features into discrete buckets.  This is often desirable for logistic regression which we'll be performing later in this lab.
# MAGIC  
# MAGIC We'll use the following splits: -infinity, -0.5, 0.0, 0.5, +infinity.  Note that in Python infinity can be represented using `float('inf')` and that in Scala `Double.NegativeInfinity` and `Double.PositiveInfinity` can be used.

# COMMAND ----------

from pyspark.ml.feature import Bucketizer

splits = [-float('inf'), -.5, 0.0, .5, float('inf')]

lengthBucketizer = (Bucketizer()
              .setInputCol('sepalLength')
              .setOutputCol('lengthFeatures')
              .setSplits(splits))

irisBucketizedLength = lengthBucketizer.transform(irisSeparateFeatures)
display(irisBucketizedLength)

# COMMAND ----------

widthBucketizer = (Bucketizer()
                   .setInputCol("sepalWidth")
                   .setOutputCol("widthFeatures")
                   .setSplits(splits))

irisBucketizedWidth = widthBucketizer.transform(irisBucketizedLength)
display(irisBucketizedWidth)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's combine the two bucketizers into a [Pipeline](http://spark.apache.org/docs/latest/ml-guide.html#pipeline-components) that performs both bucketizations.  A `Pipeline` is made up of stages which can be set using `setStages` and passing in a `list` of stages in Python or an `Array` of stages in Scala.  `Pipeline` is an estimator, which means it implements a `fit` method which returns a `PipelineModel`.  A `PipelineModel` is a transformer, which means that it implements a `transform` method which can be used to run the stages.

# COMMAND ----------

from pyspark.ml.pipeline import Pipeline

pipelineBucketizer = Pipeline().setStages([lengthBucketizer, widthBucketizer])

pipelineModelBucketizer = pipelineBucketizer.fit(irisSeparateFeatures)
irisBucketized = pipelineModelBucketizer.transform(irisSeparateFeatures)

display(irisBucketized)


# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have created two new features through bucketing, let's combine those two features into a `Vector` with `VectorAssembler`.  VectorAssembler can be found in [pyspark.ml.feature](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler) for Python and the [org.apache.spark.ml.feature](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler) package for Scala.
# MAGIC  
# MAGIC Set the params of `assembler` so that both "lengthFeatures" and "widthFeatures" are assembled into a column called "featuresBucketized".
# MAGIC  
# MAGIC Then, set the stages of `pipeline` to include both bucketizers and the assembler as the last stage.
# MAGIC  
# MAGIC Finally, use `pipeline` to generate a new `DataFrame` called `irisAssembled`.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
pipeline = Pipeline()
assembler = VectorAssembler()

print assembler.explainParams()
print '\n',pipeline.explainParams()

# COMMAND ----------

# ANSWER
# Set assembler params
(assembler
 .setInputCols(['lengthFeatures', 'widthFeatures'])
 .setOutputCol('featuresBucketized'))

pipeline.setStages([lengthBucketizer, widthBucketizer, assembler])
irisAssembled = pipeline.fit(irisSeparateFeatures).transform(irisSeparateFeatures)
display(irisAssembled)

# COMMAND ----------

# TEST
from pyspark.mllib.linalg import Vectors
firstAssembly = irisAssembled.select('lengthFeatures', 'widthFeatures', 'featuresBucketized').first()
Test.assertTrue(all(firstAssembly[2].toArray() == [firstAssembly[0], firstAssembly[1]]),
                'incorrect value for column featuresBucketized')
