// Databricks notebook source exported at Sun, 31 Jan 2016 00:29:32 UTC
//Start this demo on a Spark cluster with at least 1 Worker

// COMMAND ----------

// Where will the #5 print here? Which JVM's log?
// Answer: Driver JVM
val y = 4 + 1
println(y)

// COMMAND ----------

// First, create a Scala array
val wordList = Array("cat", "cat", "fish", "dog", "fish")

// COMMAND ----------

// Now, we'll create an RDD from the Scala array, requesting 1 partition
val wordsRDD = sc.parallelize(wordList, 1)

// COMMAND ----------

// Show # of tasks in UI + expand below
wordsRDD.count()

// COMMAND ----------

wordsRDD.partitions.length

// COMMAND ----------

// Also run this a few times and show that on the Executors UI page, the # of tasks just goes up on 1 Executor (not both)
wordsRDD.collect()

// COMMAND ----------

//Why won't this print anything?
// Answer: because there's no action
wordsRDD.map(c => println(c))

// COMMAND ----------

// Where will this print occur? 
// A: To the logs for the Executors (show students via Spark UI's Executors page)
wordsRDD.map(c => println(c)).collect()

// COMMAND ----------

val wordsRDDWith2Partitions = sc.parallelize(wordList, 2)

// COMMAND ----------

wordsRDDWith2Partitions.collect()

// COMMAND ----------

// How do we check what is in each of the two parititions?
wordsRDDWith2Partitions.map(c => println(c)).collect()

// COMMAND ----------

for (p <- wordsRDDWith2Partitions.glom.collect())
  println(p.toSeq)

// COMMAND ----------

sc

// COMMAND ----------

sc.version

// COMMAND ----------

sc.appName

// COMMAND ----------

sc.defaultParallelism

// COMMAND ----------

// Notice that this has 1 task per executor core
val testRDD = sc.parallelize(0 to 100).collect()

// COMMAND ----------

display(dbutils.fs.ls("dbfs:///mnt/training/data/tom_sawyer"))

// COMMAND ----------

// Show how the IO aligns in UI
sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).collect()

// COMMAND ----------

// What will be the type of ebook below?
val ebook = sc.textFile("dbfs:///mnt/training/data/tom_sawyer").collect()

// COMMAND ----------

// Why doesnt this work?
ebook.count()

// COMMAND ----------

ebook.getClass

// COMMAND ----------

val ebookRDD = sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2)

// COMMAND ----------

ebookRDD.count()

// COMMAND ----------

ebookRDD.partitions.length

// COMMAND ----------

ebookRDD.getClass

// COMMAND ----------

ebookRDD

// COMMAND ----------

// It simply counts the number of items in each partition.
for ((p, i) <- ebookRDD.glom.collect().zipWithIndex)
  println(s"$i: ${p.length} item(s)")

// COMMAND ----------

// Show how this is 2 operations in the UI, STILL 2 tasks! Pipelining...
sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).flatMap(line => line.split(" ")).collect()

// COMMAND ----------

sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).flatMap(line => line.split(" ")).map(word => (word, 1)).collect()


// COMMAND ----------

sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y).collect()


// COMMAND ----------

// Notice stage 2 has 4 tasks now
sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).flatMap { line => line.split(" ") }.map { word => (word, 1) }.reduceByKey((x, y) => x + y, 4).collect()


// COMMAND ----------

//why doesnt this show up?
val cachedBookRDD = sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).cache()

// COMMAND ----------

cachedBookRDD.setName("My Cached Book") 

// COMMAND ----------

// Run this twice to show speed increase
// Show size in memory of RDD
cachedBookRDD.count()

// COMMAND ----------

cachedBookRDD.unpersist()

// COMMAND ----------

//why doesnt this show up?
import org.apache.spark.storage.StorageLevel._

val cachedBookSerializedRDD = sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).persist(MEMORY_ONLY_SER)

// COMMAND ----------

cachedBookSerializedRDD.setName("My Serialized Cached Book")

// COMMAND ----------

cachedBookSerializedRDD.count()

// COMMAND ----------

// Show green cache dot in UI
sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).cache().flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y, 4).collect()


// COMMAND ----------

val tempRDD = sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).repartition(4)

// COMMAND ----------

tempRDD.partitions.length

// COMMAND ----------

// It simply counts the number of items in each partition.
for ((p, i) <- tempRDD.glom.collect().zipWithIndex)
  println(s"$i: ${p.length} item(s)")

// COMMAND ----------

sc.textFile("dbfs:///mnt/training/data/tom_sawyer", 2).repartition(4).cache().flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y, 4).collect()

// COMMAND ----------

val xRDD = sc.textFile("dbfs:///mnt/training/data/tom_sawyer").repartition(4).cache()

xRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y, 4).collect()

// COMMAND ----------

// Show how this lets you skip the earlier Stage in UI
// Change the word from Hadoop to another word like Tom
xRDD.filter(line => line.contains("Tom")).count()

// COMMAND ----------

val myRDD = xRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y, 4).collect()

// COMMAND ----------

val myRDD = xRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y, 4).sample(false, 0.01, 42).collect()
