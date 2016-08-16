# Databricks notebook source exported at Mon, 15 Feb 2016 02:08:26 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL and K-Means
# MAGIC  
# MAGIC This lab will demonstrate loading data from a file, transforming that data into a form usable with the ML and MLlib libraries, and building a k-means clustering model using both ML and MLlib.
# MAGIC  
# MAGIC Upon completing this lab you should understand how to read from and write to files in Spark, convert between `RDDs` and `DataFrames`, and build a model using both the ML and MLlib APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading the data
# MAGIC  
# MAGIC First, we need to load data into Spark.  We'll use a built-in utility to load a [libSVM file](http://www.csie.ntu.edu.tw/~cjlin/libsvm/faq.html), which is stored in an S3 bucket on AWS.  We'll use `MLUtils.loadLibSVMFile` to load our file.  Here are the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.util.MLUtils.loadLibSVMFile) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) APIs.

# COMMAND ----------

from pyspark.mllib.util import MLUtils

baseDir = '/mnt/ml-class/'
irisPath = baseDir + 'iris.scale'
irisRDD = MLUtils.loadLibSVMFile(sc, irisPath, minPartitions=20).cache()

# We get back an RDD of LabeledPoints.  Note that the libSVM format uses SparseVectors.
irisRDD.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC What if we wanted to see the first few lines of the libSVM file to see what the format looks like?

# COMMAND ----------

sc.textFile(irisPath).take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC How is this data stored across partitions?

# COMMAND ----------

print 'number of partitions: {0}'.format(irisRDD.getNumPartitions())
elementsPerPart = (irisRDD
                   .mapPartitionsWithIndex(lambda i,x: [(i, len(list(x)))])
                   .collect())
print 'elements per partition: {0}\n'.format(elementsPerPart)
irisRDD.glom().take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's convert this `RDD` of `LabeledPoints` to a `DataFrame`

# COMMAND ----------

irisDF = irisRDD.toDF()
irisDF.take(5)

# COMMAND ----------

irisDF.take(5)

# COMMAND ----------

irisDF.show(n=20, truncate=False)

# COMMAND ----------

display(irisDF)

# COMMAND ----------

print irisDF.schema, '\n'
irisDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Why were we able to convert directly from a `LabeledPoint` to a `Row`?

# COMMAND ----------

class Person(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age

personDF = sqlContext.createDataFrame([Person('Bob', 28), Person('Julie', 35)])
display(personDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Python function calls for converting a RDD into a DataFrame
# MAGIC  
# MAGIC [createDataFrame](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L342)
# MAGIC  
# MAGIC --> [_createFromRDD](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L280)
# MAGIC  
# MAGIC ----> [_inferSchema](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L221)
# MAGIC  
# MAGIC ------> [_infer_schema](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/types.py#L813)
# MAGIC  
# MAGIC --> [back to _createFromRDD](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L304)
# MAGIC  
# MAGIC ----> [toInternal](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/types.py#L533)
# MAGIC  
# MAGIC [back to createDataFrame](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L404)

# COMMAND ----------

# Our object does have a __dict__ attribute
print Person('Bob', 28).__dict__

# COMMAND ----------

personDF = sqlContext.createDataFrame([Person('Bob', 28), Person('Julie', 35)])
display(personDF)

# COMMAND ----------

# Show the schema that was inferred
print personDF.schema
personDF.printSchema()

# COMMAND ----------

from collections import namedtuple
PersonTuple = namedtuple('Person', ['name', 'age'])
personTupleDF = sqlContext.createDataFrame([PersonTuple('Bob', 28), PersonTuple('Julie', 35)])
display(personTupleDF)


# COMMAND ----------

personTupleDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform the data
# MAGIC  
# MAGIC If you look at the data you'll notice that there are three values for label: 1, 2, and 3.  Spark's machine learning algorithms expect a 0 indexed target variable, so we'll want to adjust those labels.  This transformation is a simple expression where we'll subtract one from our `label` column.
# MAGIC  
# MAGIC For help, reference the SQL Programming Guide portion on [dataframe-operations](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations) or the Spark SQL [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.package) APIs.  `select`, `col`, and `alias` can be used to accomplish this.
# MAGIC  
# MAGIC The resulting `DataFrame` should have two columns: one named `features` and another named `label`.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

irisDFZeroIndex = irisDF.select('features', (col('label') - 1).alias('label'))
display(irisDFZeroIndex)

# COMMAND ----------

# TEST
from test_helper import Test
Test.assertEquals(irisDFZeroIndex.select('label').map(lambda r: r[0]).take(3), [0, 0, 0],
                  'incorrect value for irisDFZeroIndex')

# COMMAND ----------

# MAGIC %md
# MAGIC You'll also notice that we have four values for features and that those values are stored as a `SparseVector`.  We'll reduce those down to two values (for visualization purposes) and convert them to a `DenseVector`.  To do that we'll need to create a `udf` and apply it to our dataset.  Here's a `udf` reference for [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf) and for [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.UserDefinedFunction).
# MAGIC  
# MAGIC Note that you can call the `toArray` method on a `SparseVector` to obtain an array, and you can convert an array into a `DenseVector` using the `Vectors.dense` method.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import udf
# Note that VectorUDT and MatrixUDT are found in linalg while other types are in sql.types
# VectorUDT should be the return type of the udf
from pyspark.mllib.linalg import Vectors, VectorUDT

# Take the first two values from a SparseVector and convert them to a DenseVector
firstTwoFeatures = udf(lambda sv: Vectors.dense(sv.toArray()[:2]), VectorUDT())

irisTwoFeatures = irisDFZeroIndex.select(firstTwoFeatures('features').alias('features'), 'label').cache()
display(irisTwoFeatures)

# COMMAND ----------

# TEST
Test.assertEquals(str(irisTwoFeatures.first()), 'Row(features=DenseVector([-0.5556, 0.25]), label=0.0)',
                  'incorrect definition of firstTwoFeatures')
