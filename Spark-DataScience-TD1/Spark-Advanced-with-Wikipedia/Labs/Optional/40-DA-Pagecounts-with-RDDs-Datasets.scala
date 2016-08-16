// Databricks notebook source exported at Tue, 17 May 2016 19:38:02 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC # Analyzing the Wikipedia PageCounts with RDDs and Datasets
// MAGIC ### Time to complete: 20 minutes
// MAGIC 
// MAGIC #### Business questions:
// MAGIC 
// MAGIC * Question # 1) How many unique articles in English Wikipedia were requested in the past hour?
// MAGIC * Question # 2) How many requests total did English Wikipedia get in the past hour?
// MAGIC * Question # 3) How many requests total did each Wikipedia project get total during this hour?
// MAGIC * Question # 4) How many requests did the "Apache Spark" article recieve during this hour? Which Wikipedia language got the most requests for "Apache Spark"?
// MAGIC * Question # 5) How many requests did the English Wiktionary project get during the captured hour?
// MAGIC * Question # 6) Which Apache project in English Wikipedia got the most hits during the captured hour?
// MAGIC * Question # 7) What were the top 30 pages viewed in English Wikipedia during the capture hour?
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Understand the difference between Dataframes, RDDs and Datasets
// MAGIC * Learn how to use the following RDD actions: `count`, `take`, `takeSample`, `collect`
// MAGIC * Learn the following RDD transformations: `filter`, `map`, `groupByKey`, `reduceByKey`, `sortBy`
// MAGIC * Learn how to convert your RDD code to Datasets
// MAGIC * Learn how to cache an RDD or Dataset and view its number of partitions and total size in memory
// MAGIC * Learn how to send a closure function to a map transformation
// MAGIC * Learn how to define a case class to organize data in an RDD or Dataset into objects
// MAGIC * Learn how to interpret a DAG visualization and understand the number of stages and tasks
// MAGIC * When using the RDD API, learn why groupByKey should be avoided in favor of reducebyKey
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC Dataset: https://dumps.wikimedia.org/other/pagecounts-raw/

// COMMAND ----------

// MAGIC %md Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC #![Restart cluster](http://i.imgur.com/xkRjRYy.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting to know the Data
// MAGIC Recall that we are running an hourly job in Databricks to download the latest Pagecounts file to S3 in a staging folder. How large is the data from the past hour? Let's use `%fs` to find out.

// COMMAND ----------

// MAGIC %fs ls /mnt/wikipedia-readwrite/pagecounts/staging/

// COMMAND ----------

// MAGIC %md The file is approximately 75 - 100 MB.

// COMMAND ----------

// MAGIC %md Notice that the file name has the date and time of when the file was created by the Wikimedia Foundation. This file contains recent web traffic data to Wikipedia, that is less than 1 hour old. It captures 1 hour of page counts to all of Wikipedia languages and projects.

// COMMAND ----------

// MAGIC %md
// MAGIC ### RDDs
// MAGIC RDDs can be created by using the Spark Context object's `textFile()` method.

// COMMAND ----------

// In Databricks, the SparkContext is already created for you as the variable sc
sc

// COMMAND ----------

// MAGIC %md Create an RDD from the recent pagecounts file:

// COMMAND ----------

// Notice that this returns a RDD of Strings
val pagecountsRDD = sc.textFile("dbfs:/mnt/wikipedia-readwrite/pagecounts/staging/")

// COMMAND ----------

// MAGIC %md The `count` action counts how many items (lines) total are in the RDD (this requires a full scan of the file):

// COMMAND ----------

pagecountsRDD.count()

// COMMAND ----------

// MAGIC %md The Spark UI will show that just one task read the entire file and the Input column should match the size of the file. For example, if the file were 72.4 MB, you would see:
// MAGIC #![1 task](http://i.imgur.com/Xu9LjbU.png)

// COMMAND ----------

// MAGIC %md So the count shows that there are about 5 - 7 million lines in the file. Notice that the `count()` action took 3 - 25 seconds to run b/c it had to read the entire file remotely from S3.

// COMMAND ----------

// MAGIC %md ** Challenge 1:**  Why is only one task being used to read this file? If the S3 input split is 64 MB, then why aren't two tasks being used? 

// COMMAND ----------

// Speculate upon your answer here

// gzip is an unsplittable compression format released in 1992. Therefore to uncompress a gzip file, it has to be read entirely in one machine and uncompressed together. It is not possible to parallelize this, so Spark ends up using just one task to read the file. bzip2, LZO and Snappy are  are examples of splittable compression formats.

// COMMAND ----------

// MAGIC %md You can use the take action to get the first 10 records:

// COMMAND ----------

pagecountsRDD.take(10)

// COMMAND ----------

// MAGIC %md The take command is much faster because it does not have read the entire file, it only reads 10 lines:
// MAGIC 
// MAGIC #![1 task](http://i.imgur.com/MpYvzeA.png)

// COMMAND ----------

// MAGIC %md Unfortunately results returned by `.take(10)` are not very readable because `take()` returns an array and Scala simply prints the array with each element separated by a comma. 
// MAGIC 
// MAGIC We can make the output prettier by traversing the array to print each record on its own line *(the .foreach() here is NOT a Spark operation, it's a local Scala operator)*:

// COMMAND ----------

pagecountsRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md Notice that each line in the file actually contains 2 strings and 2 numbers, but our RDD is treating each line as a long string. We'll fix this typing issue shortly by using a custom parsing function.

// COMMAND ----------

// MAGIC %md In the output above, the first column (like `aa`) is the Wikimedia project name. The following abbreviations are used for the first column:
// MAGIC ```
// MAGIC wikibooks: ".b"
// MAGIC wiktionary: ".d"
// MAGIC wikimedia: ".m"
// MAGIC wikipedia mobile: ".mw"
// MAGIC wikinews: ".n"
// MAGIC wikiquote: ".q"
// MAGIC wikisource: ".s"
// MAGIC wikiversity: ".v"
// MAGIC mediawiki: ".w"
// MAGIC ```
// MAGIC 
// MAGIC Projects without a period and a following character are Wikipedia projects. So, any line starting with the column `aa` refers to the Aragonés language Wikipedia. Similarly, any line starting with the column `en` refers to the English language Wikipedia. `en.b` refers to English Language Wikibooks.
// MAGIC 
// MAGIC The second column is the title of the page retrieved, the third column is the number of requests, and the fourth column is the size of the content returned.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Datasets
// MAGIC Datasets can be created by using the SQL Context object's `read.text()` method:

// COMMAND ----------

// Notice that this returns a Dataset of Strings
val pagecountsDS = sqlContext.read.text("dbfs:/mnt/wikipedia-readwrite/pagecounts/staging/").as[String]

// COMMAND ----------

// Notice that you get an array of Strings back
pagecountsDS.take(10)

// COMMAND ----------

pagecountsDS.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Caching RDDs vs Datasets in memory
// MAGIC Next, let's cache both the `pagecountsRDD` and `pagecountsDS` into memory and see how much space they take.

// COMMAND ----------

pagecountsRDD.setName("pagecountsRDD").cache.count // call count after the cache to force the materialization immediately

// COMMAND ----------

pagecountsDS.cache.count

// COMMAND ----------

// MAGIC %md The Spark UI's Storage tab now shows both in memory. Notice that the Dataset is compressed in memory by default, so it takes up much less space *(your exact size numbers will vary depending how the last hours's file size)*:
// MAGIC 
// MAGIC #![DS vs RDD](http://i.imgur.com/RsDpcD8.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pagecount Parsing Function
// MAGIC 
// MAGIC Storing each line in the file as a String item in the RDD or Dataset is not the most effective solution, since each line actually has 4 fields in it. 
// MAGIC 
// MAGIC Let's define a function, `parse`, to parse out the 4 fields on each line. Then we'll run the parse function on each item in the RDD or Dataset and create a new RDDs and Datasets with the correct types for each item.

// COMMAND ----------

// Define a parsing function that takes in a line string and returns the 4 fields on each line, correctly typed
def parse(line:String) = {
  val fields = line.split(' ') //Split the original line with 4 fields according to spaces
  (fields(0), fields(1), fields(2).toInt, fields(3).toLong) // return the 4 fields with their correct data types
}

// COMMAND ----------

// Now we get back a RDD with the correct types, each line has 2 strings and 2 numbers
val pagecountsParsedRDD = pagecountsRDD.map(parse)

// COMMAND ----------

// Here we get back a Dataset with the correct types, each line has 2 strings and 2 numbers
val pagecountsParsedDS = pagecountsDS.map(parse)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Revisiting caching RDDs vs Datasets in memory
// MAGIC Next, let's cache both the new `pagecountsParsedRDD` and `pagecountsParsedDS` into memory and see how much space they take.

// COMMAND ----------

pagecountsParsedRDD.setName("pagecountsParsedRDD").cache.count

// COMMAND ----------

pagecountsParsedDS.cache.count

// COMMAND ----------

// MAGIC %md The Spark UI's Storage tab now shows all four in memory (2 RDDs, 2 Datasets). Notice that the Parsed RDD takes up more space than the base RDD, but the Parsed Dataset uses less space than the base Dataset:
// MAGIC 
// MAGIC #![DS vs RDD](http://i.imgur.com/H5zMT8n.png)

// COMMAND ----------

// MAGIC %md Notice that the Parsed RDD is more costly in memory (compared to the first RDD) but  the Parsed Dataset is cheaper to store in memory (compared to the first Dataset).
// MAGIC 
// MAGIC This is because of the way Java objects are represented normally in memory. When using RDDs, Java objects are many times larger than their underlying fields, with a bunch of data structures and pointers floating around. 
// MAGIC 
// MAGIC Consider the fact that a 4 byte string with UTF-8 encoding in Java actually ends up taking 48 bytes of memory in the JVM.
// MAGIC 
// MAGIC However, Project Tungsten's UnsafeRow format is far more efficient and operates directly on binary data rather than Java objects by using `sun.misc.Unsafe`. Learn more about Project Tungsten via [Josh Rosen's YouTube video](https://www.youtube.com/watch?v=5ajs8EIPWGI) and the Reynold and Josh's [Databricks blog post](https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html).

// COMMAND ----------

// MAGIC %md
// MAGIC ### Common RDD and Dataset Transformantions and Actions
// MAGIC Next, we'll explore some common transformation and actions.

// COMMAND ----------

// MAGIC %md Consider opening the [RDD API docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) and [Dataset API docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) in new tabs to keep them handy. Remember that you can also hit 'tab' after the RDD or Dataset name to see a drop down of the available methods.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #1: 
// MAGIC ** How many unique articles in English Wikipedia were requested in the past hour?**

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

// MAGIC %md Let's filter out just the lines referring to English Wikipedia:

// COMMAND ----------

// Note: _._1 is just scala syntax for yanking out the first element from each line
val enPagecountsRDD = pagecountsParsedRDD.filter(_._1 == "en")

// COMMAND ----------

// MAGIC %md Note that the above line is lazy and doesn't actually run the filter. We have to trigger the filter transformation to run by calling an action:

// COMMAND ----------

enPagecountsRDD.count()

// COMMAND ----------

// MAGIC %md Around 2 million lines refer to the English Wikipedia project. So about half of the 5 million articles in English Wikipedia get requested per hour. Let's take a look at 10 random lines:

// COMMAND ----------

enPagecountsRDD.takeSample(true, 10).foreach(println)

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

// MAGIC %md Running a filter and count on a Dataset looks very similar:

// COMMAND ----------

val enPagecountsDS = pagecountsParsedDS.filter(_._1 == "en")

// COMMAND ----------

enPagecountsDS.count()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #2:
// MAGIC ** How many requests total did English Wikipedia get in the past hour?**

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

// MAGIC %md Start with the `enPagecountsRDD`:

// COMMAND ----------

enPagecountsRDD.take(5)

// COMMAND ----------

// MAGIC %md ** Challenge 2:** Can you figure out how to yank out just the requests column and then sum all of the requests?

// COMMAND ----------

// Type your answer here... Yank out just the requests column
enPagecountsRDD.map(x => x._3).take(5)

// COMMAND ----------

// Type your answer here... Then build upon that by summing up all of the requests
enPagecountsRDD.map(x => x._3).sum

// COMMAND ----------

// MAGIC %md We can see that there were between 5 - 10 million requests to English Wikipedia in the past hour.

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

// MAGIC %md Let's re-write the same query using Datasets:

// COMMAND ----------

// The map() operation looks the same as the RDD version
enPagecountsDS.map(x => x._3).take(5)

// COMMAND ----------

// MAGIC %md Note that there is no available `.sum()` method on Datasets:

// COMMAND ----------

// This will return an error
enPagecountsDS.map(x => x._3).sum

// COMMAND ----------

// MAGIC %md The Datasets API in Spark 1.6 is still experimental, so full functionality is not available yet.

// COMMAND ----------

// MAGIC %md #### Strategy #1) Collect on Driver and sum locally

// COMMAND ----------

// MAGIC %md Instead, if the data is small enough, we can collect it on the Driver and sum it locally.
// MAGIC 
// MAGIC ** Challenge 3:** Implement this new strategy of collecting the data on the Driver for the summation.

// COMMAND ----------

// Type your answer here...

enPagecountsDS.map(x => x._3).collect.sum

// COMMAND ----------

// MAGIC %md Performance here may appear fast in a local mode cluster because no network transfer has to take place. Also, collecting data at the driver to perform a sum won't scale if the data set is too large to fit on one machine (which could cause an Out of Memory condition).

// COMMAND ----------

// MAGIC %md #### Strategy #2) Convert DS to a DF for the sum

// COMMAND ----------

// MAGIC %md Another strategy is to convert the Dataset to a Dataframe just to perform the sum.

// COMMAND ----------

// MAGIC %md ** Challenge 4:** See if you can start with the `enPagecountsDS` Dataset, run a map on it like above, then convert it to a Dataframe and sum the `value` column.

// COMMAND ----------

// Type your answer here...
// Hint: Remember to import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions._

enPagecountsDS 
  .map(x => x._3)
  .toDF
  .select(sum($"value"))
  .show()

// COMMAND ----------

// MAGIC %md #### Strategy #3) Implement a custom Aggregator for sum

// COMMAND ----------

// MAGIC %md In the final strategy, we construct a simple Aggregator that sums up a collection of `Int`s.

// COMMAND ----------

// MAGIC %md Aggregators provide a mechanism for adding up all of the elements in a Dataset, returning a single result. An Aggregator is similar to a User Defined Aggregate Function (UDAF), but the interface is expressed in terms of JVM objects instead of as a Row.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.TypedColumn

// COMMAND ----------

val simpleSum = new Aggregator[Int, Int, Int] with Serializable {
  def zero: Int = 0                     // The initial value.
  def reduce(b: Int, a: Int) = b + a    // Add an element to the running total
  def merge(b1: Int, b2: Int) = b1 + b2 // Merge intermediate values.
  def finish(b: Int) = b                // Return the final result.
}.toColumn

// COMMAND ----------

// Why is this so slow? This cell takes about 1 minute to complete! We will optimize this next.
enPagecountsDS.map(x => x._3).select(simpleSum).collect

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Performance Optimization: Understanding the relationship between # of partitions and # of tasks

// COMMAND ----------

// MAGIC %md The slow Spark job above launches two stages and one task in each stage. Recall that each local mode cluster in Databricks has 4 slots, so 4 tasks can be run simultaneously.

// COMMAND ----------

// MAGIC %md Let's repartition the Dataset from 1 partition to 4 partitions so that we can run 4 tasks in parallel when analyzing it:

// COMMAND ----------

val pagecounts4PartitionsDS = pagecountsParsedDS.repartition(4).cache

// COMMAND ----------

pagecounts4PartitionsDS.count // Materialize the cache

// COMMAND ----------

// MAGIC %md The new Dataset with 4 partitions should now be materialized:
// MAGIC 
// MAGIC #![4 partitions DS](http://i.imgur.com/zByahcZ.png)

// COMMAND ----------

// The same operations now complete in about 15-20 seconds, when reading from 4 partitions in memory
pagecounts4PartitionsDS.filter(_._1 == "en" ).map(x => x._3).select(simpleSum).collect

// COMMAND ----------

// MAGIC %md The second stage in the command above runs four tasks in parallel to compute the results.

// COMMAND ----------

// MAGIC %md Go ahead and re-partition the `pagecountsParsedRDD` into 4 partitions also, for similar speed increases:

// COMMAND ----------

val pagecounts4PartitionsRDD = pagecountsParsedRDD.repartition(4).setName("pagecounts4PartitionsRDD").cache

// COMMAND ----------

pagecounts4PartitionsRDD.count // Materialize the cache

// COMMAND ----------

// MAGIC %md The new RDD with 4 partitions should now be materialized:
// MAGIC 
// MAGIC #![4 partitions RDD](http://i.imgur.com/GkXqg9I.png)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #3:
// MAGIC ** How many requests total did each Wikipedia project get total during this hour?**

// COMMAND ----------

// MAGIC %md Recall that our data file contains requests to all of the Wikimedia projects, including Wikibooks, Wiktionary, Wikinews, Wikiquote... and all of the 200+ languages.

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

// MAGIC %md ** Challenge 5:** First, we'll create key/value pairs from the project prefix and the number of requests, so we want to see results like: `((en, 3), (en.b, 2), (aa, 2), (en, 7))`. Can you use a map operation to get an RDD back with just k/v pairs?

// COMMAND ----------

// Type you answer here...

pagecounts4PartitionsRDD
  .map(line => (line._1, line._3))
  .take(10)

// COMMAND ----------

// MAGIC %md Then try calling groupByKey() to gather all of the similar project prefixes together and then sum them using map():

// COMMAND ----------

pagecounts4PartitionsRDD
  .map(line => (line._1, line._3))
  .groupByKey()
  .map(kv => (kv._1, kv._2.sum))
  .collect()

// COMMAND ----------

// MAGIC %md That collected a lot of data at the Driver. To make the results more readable, let's sort by the number of requests, from highest to lowest and just display the top ten projects:

// COMMAND ----------

// Sort by the value (number of requests) and pass in false to sort in descending order
pagecounts4PartitionsRDD
  .map(line => (line._1, line._3))
  .groupByKey()
  .map(kv => (kv._1, kv._2.sum))
  .sortBy(kv => kv._2, false)
  .take(10)
  .foreach(println)

// COMMAND ----------

// MAGIC %md We can see that the English Wikipedia Desktop and the English Wikipedia Mobile got the most hits this hour, followed by some other languages (usually depending on where the sun is currently up, check out: http://www.die.net/earth/).

// COMMAND ----------

// MAGIC %md The previous command is a bit slow... it takes about 8-9 seconds to run.

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Performance Optimization: Use reduceByKey() instead of groupByKey()

// COMMAND ----------

// MAGIC %md Actually, we can also use `reduceByKey()` to calculate the answer much faster:

// COMMAND ----------

pagecounts4PartitionsRDD
  .map(line => (line._1, line._3))
  .reduceByKey(_ + _)
  .sortBy(x => x._2, false)
  .take(10)
  .foreach(println)

// COMMAND ----------

// MAGIC %md Curious about why `reduceByKey()` is more efficient?
// MAGIC 
// MAGIC Check out the 
// MAGIC [Databricks Knowledge Base](https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html) for a quick explanation.

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

// MAGIC %md When using Datasets, you don't have to worry about picking the perfect Spark operation (like `reduceByKey()` above). Instead, the Catalyst Optimizer will pick the most performant physical plan automatically!

// COMMAND ----------

// MAGIC %md Start by creating key/value pairs from the project prefix and the number of requests:

// COMMAND ----------

pagecounts4PartitionsDS.map(line => (line._1, line._3)).take(5)

// COMMAND ----------

pagecounts4PartitionsDS.map(line => (line._1, line._3)).groupBy(_._1).count().take(5)

// COMMAND ----------

// MAGIC %md Since Datasets are still an experimental API and aggregations/sorting are not yet fully supported, let's switch the Dataset to a Dataframe for an aggregation:

// COMMAND ----------

pagecounts4PartitionsDS
  .map(line => (line._1, line._3))     // yank out k/v pairs of the project and # of requests
  .toDF()                              // Convert to DataFrame to perform aggregation / sorting
  .groupBy($"_1")                      // Group the k/v pairs by the key (project name)
  .agg(sum("_2") as "sumOccurances")   // Sum up how many occurrances there are of each project
  .orderBy($"sumOccurances" desc)      // Order in descening order
  .take(10)
  .foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #4:
// MAGIC ** How many requests did the "Apache Spark" article recieve during this hour? Which Wikipedia language got the most requests for "Apache Spark"?**

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

// MAGIC %md Using our existing RDDs and Datasets are kind of confusing to work with because we have not named the columns. We've been having to refer to columns using arcane syntax like `._1` or `$"_1"`. 
// MAGIC 
// MAGIC So, let's define a case class to organize our data in PageCount objects with named + typed columns:

// COMMAND ----------

case class PageCount(val project: String, val title: String, val requests: Long, val size: Long) extends java.io.Serializable

// COMMAND ----------

val pagecountObjectsRDD = pagecountsRDD
  .map(_.split(' '))
  .filter(_.size == 4)
  .map(pc => new PageCount(pc(0), pc(1), pc(2).toLong, pc(3).toLong))
  .repartition(4)
  .setName("pagecountObjectsRDD")
  .cache()

// COMMAND ----------

pagecountObjectsRDD.count // Materialize the cache

// COMMAND ----------

// MAGIC %md The new pagecountObjectsRDD with 4 partitions which contains the named + type columns takes up a lot more space in memory than the RDD with just typed columns:
// MAGIC 
// MAGIC #![RDD comparison](http://i.imgur.com/xk4PFOk.png)

// COMMAND ----------

// MAGIC %md Filter out just the lines that mention "Apache_Spark" in the title:

// COMMAND ----------

// Note that now we can refer to the fields on each line with its friendly name, for example title here
pagecountObjectsRDD
  .filter(_.title.contains("Apache_Spark"))
  .count

// COMMAND ----------

// MAGIC %md The number you see in the cell above is how many different lines in this hour's pagecounts file refer to the "Apache Spark" article. 

// COMMAND ----------

// MAGIC %md ** Challenge 6:** Can you figure out which language edition of the Apache Spark page got the most hits? 
// MAGIC 
// MAGIC Hint: Consider using a .map() after the filter() in the cell above.

// COMMAND ----------

// Type your answer here...
pagecountObjectsRDD
  .filter(_.title.contains("Apache_Spark"))
  .map(x => (x.project, x.requests))
  .collect

// COMMAND ----------

// MAGIC %md It seems like the English version of the Apache Spark page got the most hits by far.

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

// MAGIC %md First use the PageCount case class to create a Dataset named `pagecountObjectsDS`:

// COMMAND ----------

val pagecountObjectsDS = pagecountsDS
  .filter(_.split(' ').size == 4)
  .map { line =>
    val fields = line.split(' ')
    PageCount(fields(0), fields(1), fields(2).toLong, fields(3).toLong)
  }
  .repartition(4)
  .cache()

// COMMAND ----------

// MAGIC %md Then do the calculation:

// COMMAND ----------

// Notice how similar this code looks to the RDD version a few cells above
pagecountObjectsDS
  .filter(_.title.contains("Apache_Spark"))
  .map(x => (x.project, x.requests))
  .collect

// COMMAND ----------

// MAGIC %md At this time, check out the Spark UI and compare the `pagecountObjectsDS` in memory to the `pagecounts4PartitionsDS`, which was parsed fields with just type info (not col names). Note both have 4 partitions. You will notice that when adding a column name to the 4 partitions RDD, it blew up in memory space by 3-4x. Datasets however, don't do this.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #5:
// MAGIC ** How many requests did the English Wiktionary project get during the captured hour?**

// COMMAND ----------

// MAGIC %md 
// MAGIC The [Wiktionary](https://en.wiktionary.org/wiki/Wiktionary:Main_Page) project is a free dictionary with 4 million+ entries from over 1,500 languages.

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

// MAGIC %md ** Challenge 7:** Can you figure this out? Start by figuring out the correct prefix that identifies the English Wikitionary project.
// MAGIC 
// MAGIC Hint: This one's easy!

// COMMAND ----------

// Type your answer here...
pagecountObjectsRDD
  .filter(_.project.contains("en.d"))
  .count

// COMMAND ----------

// MAGIC %md The English Wikionary project gets approximately 100,000 requests each hour.

// COMMAND ----------

// MAGIC %md Note that RDDs have compile-time type safety and will complain right away if you issue a bad column name or make a comparison between incompatible data types:

// COMMAND ----------

// This command will fail because the "badColName" does not exist
pagecountObjectsRDD
  .filter(_.badColName.contains("en.d"))

// COMMAND ----------

// This command will fail because it's not possible to subtract a number (50) from a String (_.project)
pagecountObjectsRDD
  .filter(_.project - 50)

// COMMAND ----------

// MAGIC %md Note that the above 2 errors are caught at compile time by the Scala compiler.

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

pagecountObjectsDS
  .filter(_.project.contains("en.d"))
  .count

// COMMAND ----------

// MAGIC %md Similar to RDDs, Datasets also have compile-time type safety and will complain right away if you issue a bad column name make a comparison between incompatible data types:

// COMMAND ----------

// This command will fail because the "badColName" does not exist
pagecountObjectsDS
  .filter(_.badColName.contains("en.d"))

// COMMAND ----------

// This command will fail because it's not possible to subtract a number (50) from a String (_.project)
pagecountObjectsDS
  .filter(_.project - 50)

// COMMAND ----------

// MAGIC %md Note that the above 2 errors are caught at compile time by the Scala compiler before the query plan gets sent to the Catalyst optimizer.

// COMMAND ----------

// MAGIC %md ##### Dataframe answer:

// COMMAND ----------

// MAGIC %md Let's write a solution for this question via Dataframes API also for a comparison:

// COMMAND ----------

display(pagecountObjectsDS.toDF)

// COMMAND ----------

// Here is the answer using Dataframes. The syntax for the filter is a bit different
pagecountObjectsDS
  .toDF
  .filter($"project" === "en.d")
  .count

// COMMAND ----------

// MAGIC %md Unlike RDDs and Datasets, Dataframes are not compile time type-safe:

// COMMAND ----------

// Here we are trying to access an invalid column name and the Catalyst optimizer throws an error
pagecountObjectsDS
  .toDF
  .filter($"badColName" === "en.d")
  .count

// COMMAND ----------

// Here we are trying to access an invalid column name and the Catalyst optimizer throws an error
pagecountObjectsDS
  .toDF
  .filter($"project" - 50)

// COMMAND ----------

// MAGIC %md The above errors are run time errors thrown by Spark SQL (it was not caught by Scala at compile time and got sent to Catalyst, which threw an error). Notice how the error is different here than the RDD and Dataset errors above.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #6:
// MAGIC ** Which Apache project in English Wikipedia got the most hits during the captured hour?**

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

// Here we reuse the pagecountObjectsRDD we had defined earlier
pagecountObjectsRDD
  .filter(_.project.contains("en"))
  .filter(_.title.contains("Apache_"))
  .map(x => (x.title, x.requests))
  .collect
  .foreach(println)

// COMMAND ----------

// MAGIC %md The results above are not sorted. Let's sort them by the value, from highest to lowest:

// COMMAND ----------

// Here we reuse the pagecountObjectsRDD we had defined earlier
pagecountObjectsRDD
  .filter(_.project.contains("en"))
  .filter(_.title.contains("Apache_"))
  .map(x => (x.title, x.requests))
  .map(item => item.swap) // interchanges position of entries in each tuple
  .sortByKey(false, 1) // 1st arg configures ascending sort, 2nd arg configures one task
  .map(item => item.swap)
  .collect
  .foreach(println)

// COMMAND ----------

// MAGIC %md We can infer from the above results which Apache projects were the most popular in the past hour.

// COMMAND ----------

// MAGIC %md ##### Dataset + Dataframe answer:

// COMMAND ----------

// MAGIC %md Currently, we have to convert a Dataset to a Dataframe to perform aggregation or sorting operations. Display the Dataframe before the sort:

// COMMAND ----------

display(pagecountObjectsDS
  .filter(_.project.contains("en"))
  .filter(_.title.contains("Apache_"))
  .map(x => (x.title, x.requests))
  .toDF)

// COMMAND ----------

// MAGIC %md ** Challenge 8:** Sort the Dataframe above in descending order of the value column (_2):

// COMMAND ----------

// Type your answer where it says <<fill in here>> below

display(pagecountObjectsDS
  .filter(_.project.contains("en"))
  .filter(_.title.contains("Apache_"))
  .map(x => (x.title, x.requests))
  .toDF
  .orderBy($"_2".desc))//<<fill in here>>

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #7:
// MAGIC ** What were the top 30 articles viewed in English Wikipedia during the capture hour?**

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

//Recall that we already have a RDD created that we can use for this analysis
pagecountObjectsRDD

// COMMAND ----------

pagecountObjectsRDD
  .filter(_.project.contains("en"))
  .map(x => (x.title, x.requests))
  .map(item => item.swap) // interchanges position of entries in each tuple
  .sortByKey(false, 1) // 1st arg configures ascending sort, 2nd arg configures one task
  .map(item => item.swap)
  .take(30)
  .foreach(println)

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

// Notice how much simpler the Dataset answer is compared to the RDD answer above, and it'll get even simpler when you don't have to convert to a DF for aggregations in the future (Spark 2.0)!

pagecountObjectsDS
  .filter(_.project.contains("en"))
  .map(x => (x.title, x.requests))
  .toDF
  .orderBy($"_2".desc)
  .show(30)

// COMMAND ----------

// MAGIC %md This concludes the lab.
