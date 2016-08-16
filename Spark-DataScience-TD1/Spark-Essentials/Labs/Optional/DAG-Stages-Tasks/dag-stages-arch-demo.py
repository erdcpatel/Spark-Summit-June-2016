# Databricks notebook source exported at Thu, 14 Jan 2016 00:08:58 UTC
# First, create a Python list
wordList = ["doug", "jon", "sameer", "eliano", "richard"]

# COMMAND ----------

# Now, we'll create an RDD from the Python list with 4 partitions
wordsRDD = sc.parallelize(wordList, 4)

# COMMAND ----------

# Why won't this print anything?
def println(value):
   print value
    
wordsRDD.map(println)

# COMMAND ----------

# This will print something... but where?
wordsRDD.map(println).collect()
# Let's look in (executor) logs

# COMMAND ----------

# Now, we'll create an RDD from the array, requesting 6 partitions
wordsRDD = sc.parallelize(wordList, 6)

# COMMAND ----------

wordsRDD.collect()

# COMMAND ----------

sc

# COMMAND ----------

sc.version

# COMMAND ----------

sc.appName

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

datapath = "dbfs:/mnt/training/data/tom_sawyer" # https://www.gutenberg.org/ebooks/74

# COMMAND ----------

sc.textFile(datapath, 2).collect()

# COMMAND ----------

# Note, collect does not return an RDD (it returns an Array)
ebook = sc.textFile(datapath, 2).collect()

# COMMAND ----------

# Why doesnt this work?
ebook.count()

# COMMAND ----------

print 'type of ebook: {0}'.format(type(ebook))

# COMMAND ----------

# Why doesnt this show up in the UI
ebookRDD = sc.textFile(datapath, 2)

# COMMAND ----------

ebookRDD.count()

# COMMAND ----------

ebookRDD.getNumPartitions()

# COMMAND ----------

print 'type of ebookRDD: {0}'.format(type(ebookRDD))

# COMMAND ----------

ebookRDD

# COMMAND ----------

# Let's use RDD.glom to look at the distribution of data in our partitions

# What does glom do? Replaces each partition in the RDD with a single container that contains all of the partition's previous items
# http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.glom

glomDemo = sc.parallelize([1,2,3,4,5,6,7,8,9], 3)
glomDemo.glom().collect()

# How does it work? This is a good one to guess about, and then check the source

# COMMAND ----------

ebookRDD.glom().collect()

# COMMAND ----------

# Count the number of items in each partition.
for (p, i) in ebookRDD.glom().zipWithIndex().collect():
  print '%d: %d items(s)' % (i, len(p))

# COMMAND ----------

sc.textFile(datapath, 2).flatMap(lambda x: x.split(' ')).collect()

# COMMAND ----------

sc.textFile(datapath, 2).flatMap(lambda x: x.split(' ')).map(lambda s: (s, 1)).collect()

# COMMAND ----------

sc.textFile(datapath, 2).flatMap(lambda x: x.split(' ')).map(lambda s: (s, 1)).reduceByKey(lambda x, y: x + y).collect()

# COMMAND ----------

# why doesnt this show up?
cachedBookRDD = sc.textFile(datapath, 2).cache()

# COMMAND ----------

cachedBookRDD.setName("My Cached Book") 

# COMMAND ----------

cachedBookRDD.count()

# COMMAND ----------

cachedBookRDD.unpersist()

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
cachedBookSerializedRDD = sc.textFile(datapath, 2).persist(StorageLevel.MEMORY_ONLY_SER) #same thing as above, since we're using Python

# COMMAND ----------

cachedBookSerializedRDD.setName("My Serialized Cached Book")

# COMMAND ----------

cachedBookSerializedRDD.count()

# COMMAND ----------

sc.textFile(datapath, 2).flatMap(lambda x: x.split(' ')).map(lambda s: (s, 1)).reduceByKey(lambda x, y: x + y).collect()

# COMMAND ----------

tempRDD = sc.textFile(datapath, 2).repartition(4)

# COMMAND ----------

tempRDD.getNumPartitions()

# COMMAND ----------

# It simply counts the number of items in each partition.
for (p, i) in tempRDD.glom().zipWithIndex().collect():
  print '%d: %d items(s)' % (i, len(p))

# COMMAND ----------

sc.textFile(datapath, 2).repartition(12).cache().flatMap(lambda x: x.split(' ')).map(lambda s: (s, 1)).reduceByKey(lambda x, y: x + y).collect()

# COMMAND ----------

sc.textFile(datapath, 2).repartition(4).cache().flatMap(lambda x: x.split(' ')).map(lambda s: (s, 1)).reduceByKey(lambda x, y: x + y).collect()

# COMMAND ----------

myRDD = sc.textFile(datapath, 2).repartition(12).cache().flatMap(lambda x: x.split(' ')).map(lambda s: (s, 1)).reduceByKey(lambda x, y: x + y, numPartitions=8)
myRDD.collect()

# COMMAND ----------

myRDD.getNumPartitions()

# COMMAND ----------

myRDD.count() 
# this job uses data that is downstream from a shuffle we recently did -- Spark should use those shuffle output files to take a shortcut
# let's look in the UI for skipped stages
