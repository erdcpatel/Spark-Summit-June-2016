// Databricks notebook source exported at Mon, 10 Aug 2015 15:45:02 UTC
// MAGIC %md # Solutions for RDD Lab (Scala)

// COMMAND ----------

// MAGIC %md ## Exercise 1

// COMMAND ----------

// TAKE NOTE: We are deliberately only keeping the first five fields of
// each line, since that's all we're using in this lab. There's no sense
// in dragging around more data than we need.
case class CrimeData(ccn: String, 
                     reportTime: String,
                     shift: String,
                     offense: String,
                     method: String)
                     
val dataRDD = noHeaderRDD.map { line =>
  val cols = line.split(",")
  CrimeData(cols(0), cols(1), cols(2), cols(3), cols(4))
}
dataRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md ## Exercise 2

// COMMAND ----------

val groupedByOffenseRDD = dataRDD.groupBy { data => data.offense }

// What does this return? You'll need to know for the next step.
groupedByOffenseRDD.take(1).foreach(println)

// COMMAND ----------

// MAGIC %md ## Exercise 3

// COMMAND ----------

// Here's one way to do it:

val offenseCounts = groupedByOffenseRDD.map(g => (g._1, g._2.toSeq.length)).collect()
for ((offense, count) <- offenseCounts) {
  println(f"$offense%30s $count%5d")
}

// COMMAND ----------

// But here's a better way. Note the use of Scala's "f" string interpolator,
// which is kind of like printf, but with compile-time type safety. For
// more information on the "f" interpolator, see
// http://docs.scala-lang.org/overviews/core/string-interpolation.html

val offenseCounts = dataRDD.map(item => (item.offense, item)).countByKey()
for ((offense, count) <- offenseCounts) {
  println(f"$offense%30s $count%5d")
}

// COMMAND ----------

// MAGIC %md ## Exercise 4

// COMMAND ----------

println(baseRDD.partitions.length)
println(groupedByOffenseRDD.partitions.length)

// COMMAND ----------

// MAGIC %md ## Exercise 5

// COMMAND ----------

// It's okay to cache the base RDD. It's not very large. Proof:
println(s"Count of lines: ${baseRDD.count}")
val totalChars = baseRDD.map(_.length).reduce(_ + _)
println(s"Count of characters (Unicode): ${totalChars}")

// They're all about the same size, since we didn't filter any data out. However,
// since we're mostly working with the `data_rdd`, that's the one to cache.
dataRDD.cache()

// COMMAND ----------

// MAGIC %md ## Exercise 6

// COMMAND ----------

val resultRDD1 = dataRDD.filter(_.offense == "HOMICIDE").map(item => (item.method, 1)).reduceByKey(_ + _)

for ((method, count) <- resultRDD1.collect())
  println(f"$method%10s $count%d")

// COMMAND ----------

// MAGIC %md ## Exercise 7

// COMMAND ----------

// There are quite a few ways to solve this one, but here's a straightforward (and relatively fast) one.
println(dataRDD.map(item => (item.shift, 1))
               .reduceByKey(_ + _)
               .max)