// Databricks notebook source exported at Mon, 28 Dec 2015 17:38:47 UTC
// MAGIC %md
// MAGIC # RDD Lab (Scala)
// MAGIC 
// MAGIC In this lab, we'll explore some of the RDD concepts we've discussed. We'll be using a data set consisting of reported crimes in Washington D.C. in 2013. This data comes from the [District of Columbia's Open Data Catalog](http://data.octo.dc.gov/). We'll use this data to explore some RDD transitions and actions.
// MAGIC 
// MAGIC ## Exercises and Solutions
// MAGIC 
// MAGIC This notebook contains a number of exercises. Use the API docs for methods 
// MAGIC <a href="http://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.rdd.RDD" target="_blank">common to all RDDs</a>,
// MAGIC plus the extra methods for 
// MAGIC <a href="http://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions" target="_blank">pair RDDs</a>, to look up transformations and actions. If, at any point, you're struggling with the solution to an exercise, feel free to look in the **Solutions** notebook (in the same folder as this lab).
// MAGIC 
// MAGIC ## Let's get started.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load the data
// MAGIC 
// MAGIC The first step is to load the data. Run the following cell to create an RDD containing the data.

// COMMAND ----------

val baseRDD = sc.textFile("dbfs:/mnt/training/wash_dc_crime_incidents_2013.csv")

// COMMAND ----------

// MAGIC %md **Question**: Does the RDD _actually_ contain the data right now?

// COMMAND ----------

// MAGIC %md
// MAGIC ## Explore the data
// MAGIC 
// MAGIC Let's take a look at some of the data.

// COMMAND ----------

baseRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md Okay, there's a header. We'll need to remove that. But, since the file will be split into partitions, we can't just drop the first item. Let's figure out another way to do it.

// COMMAND ----------

val noHeaderRDD = baseRDD.filter { line => ! (line contains "REPORTDATETIME") }

// COMMAND ----------

noHeaderRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 1
// MAGIC 
// MAGIC Let's make things a little easier to handle, by converting the `noHeaderRDD` to an RDD containing Scala objects.
// MAGIC 
// MAGIC **TO DO**
// MAGIC 
// MAGIC * Split each line into its individual cells.
// MAGIC * Map the RDD into another RDD of appropriate `CrimeData` objects.

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
  <FILL-IN>
}.repartition(8)

// Why repartition? We'll achieve better parallelism and work around a temporary glitch that affects our class clusters.

dataRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 2
// MAGIC 
// MAGIC Next, group the data by type of crime (the "OFFENSE" column).

// COMMAND ----------

val groupedByOffenseRDD = dataRDD.groupBy { data => <FILL-IN> }

// What does this return? You'll need to know for the next step.
groupedByOffenseRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 3
// MAGIC Next, create an RDD that counts the number of each offense. How many murders were there in 2013? How many assaults with a dangerous weapon?

// COMMAND ----------

val offenseCounts = <FILL-IN>
for ((offense, count) <- offenseCounts) {
  println(<FILL-IN>)
}

// COMMAND ----------

// MAGIC %md ### Question
// MAGIC 
// MAGIC Run the following cell. Can you explain what happened? Is `collectAsMap()` a _transformation_ or an _action_?

// COMMAND ----------

groupedByOffenseRDD.collectAsMap()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 4
// MAGIC 
// MAGIC How many partitions does the base RDD have? What about the `groupedByOffenseRDD` RDD? How can you find out?
// MAGIC 
// MAGIC **Hint**: Check the [API documentation](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD).

// COMMAND ----------

println(baseRDD.<FILL-IN>)
println(groupedByOffenseRDD.<FILL-IN>)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 5
// MAGIC 
// MAGIC Since we're continually playing around with this data, we might as well cache it, to speed things up.
// MAGIC 
// MAGIC **Question**: Which RDD should you cache? 
// MAGIC 
// MAGIC 1. `baseRDD`
// MAGIC 2. `noHeaderRDD`
// MAGIC 3. `dataRDD`
// MAGIC 4. None of them, because they're all still too big.
// MAGIC 5. It doesn't really matter.

// COMMAND ----------

<FILL-IN>.cache()

// COMMAND ----------

// MAGIC %md ### Exercise 6
// MAGIC 
// MAGIC Display the number of homicides by weapon (method).

// COMMAND ----------

val resultRDD1 = dataRDD.<FILL-IN>
println(resultRDD1.collect())

// BONUS: Make the output look better, using a for loop or a foreach.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 7
// MAGIC 
// MAGIC During which police shift did the most crimes occur in 2013?

// COMMAND ----------

// Hint: Start with the dataRDD
println(dataRDD.<FILL-IN>)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![Stop Sign](http://i.imgur.com/RdABwEB.png)
// MAGIC 
// MAGIC ## Stop Here
// MAGIC 
// MAGIC We'll resume after we've discussed DataFrames.

// COMMAND ----------

// MAGIC %md ### Demonstration
// MAGIC 
// MAGIC Let's plot murders by month. DataFrames are useful for this one.

// COMMAND ----------

// MAGIC %md To do this properly, we'll need to parse the dates. That will require knowing their format. A quick sampling of the data will help.

// COMMAND ----------

dataRDD.map(_.reportTime).take(30)

// COMMAND ----------

// MAGIC %md Okay. We can now parse the strings into actual `Date` objects.
// MAGIC 
// MAGIC **NOTE:** The DataFrame API does _not_ support schemas with `Date` objects in them. We'll need to convert the resulting `Date` to a `java.sql.Timestamp`.

// COMMAND ----------

// MAGIC %md Let's set up the date/time parser.

// COMMAND ----------

import java.text.SimpleDateFormat

val dateFmt = new SimpleDateFormat("M/dd/yyyy KK:mm:SS a")


// COMMAND ----------

// MAGIC %md Now, we can create the data frame. We'll start with the `no_header_rdd` and map it slightly differently than we did to create `data_rdd`:

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import sqlContext.implicits._
import java.sql.Timestamp
import java.util.Calendar

val df = noHeaderRDD.map { line =>
  val cols = line.split(",")
  val dateString = cols(1)
  val reportTime = new Timestamp(dateFmt.parse(dateString).getTime)
  (cols(0), reportTime, cols(2), cols(3), cols(4))
}.toDF("ccn", "reportTime", "shift", "offense", "method")


// COMMAND ----------

df.printSchema()

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md Let's use a user-defined function (something supported by DataFrames and Spark SQL) to give us a `month` function we can use to extract just the month part of a `Timestamp`.

// COMMAND ----------

val month = sqlContext.udf.register("month", (t: Timestamp) => {
  val calendar = Calendar.getInstance()
  calendar.setTimeInMillis(t.getTime)
  calendar.get(Calendar.MONTH)
})


// COMMAND ----------

display( 
  df.filter($"offense" === "HOMICIDE")
    .select(month($"reportTime").as("month"), $"offense")
    .groupBy($"month").count()
)

// COMMAND ----------

// MAGIC %md What about all crimes per month?

// COMMAND ----------

display( df.select(month($"reportTime").as("month")).groupBy("month").count() )

// COMMAND ----------

// MAGIC %md We can also plot the frequency of crimes by hour of day.

// COMMAND ----------

val hour = sqlContext.udf.register("hour", (t: Timestamp) => {
  val calendar = Calendar.getInstance()
  calendar.setTimeInMillis(t.getTime)
  calendar.get(Calendar.HOUR_OF_DAY)
})

// COMMAND ----------

display(df.select(hour($"reportTime").as("hour"), $"offense").groupBy($"hour").count())

// COMMAND ----------


