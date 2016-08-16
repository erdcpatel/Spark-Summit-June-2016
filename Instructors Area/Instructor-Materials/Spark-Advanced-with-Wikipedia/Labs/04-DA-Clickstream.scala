// Databricks notebook source exported at Wed, 1 Jun 2016 22:58:04 UTC
// MAGIC %md ![Wikipedia/Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark.png)  
// MAGIC 
// MAGIC #### Analyze Feb 2015 Wikipedia Clickstream
// MAGIC 
// MAGIC **Objective:**
// MAGIC Study the Wikipedia Clickstream
// MAGIC 
// MAGIC **Time to Complete:**
// MAGIC 30 mins
// MAGIC 
// MAGIC **Data Source:**
// MAGIC Clickstream (<a href="http://datahub.io/dataset/wikipedia-clickstream/resource/be85cc68-d1e6-4134-804a-fd36b94dbb82" target="_blank">1.2 GB</a>)
// MAGIC 
// MAGIC **Business Questions:**
// MAGIC 
// MAGIC * Question # 1) How much traffic did Google send to the "Apache Spark" article in Feb 2015?
// MAGIC * Question # 2) How many unique articles did Google send traffic to?
// MAGIC * Question # 3) What are the top 10 articles requested from Wikipedia?
// MAGIC * Question # 4) Who sent the most traffic to Wikipedia in Feb 2015?
// MAGIC * Question # 5) What percentage of the traffic Wikipedia received came from other English Wikipedia pages?
// MAGIC * Question # 6) What were the top 5 trending articles on Twitter?
// MAGIC * Question # 7) What are the most requested missing pages?
// MAGIC * Question # 8) What does the traffic inflow vs outflow look like for the most requested pages?
// MAGIC * Question # 9) What does the traffic flow pattern look like for the "San Diego" article? Create a visualization for this.
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Learn how to use the Spark CSV Library to read structured files
// MAGIC * Use `%sh` to run shell commands
// MAGIC * Learn about Spark's architecture and JVM sizing
// MAGIC * Use `jps` to list Java Virtual Machines
// MAGIC * Repartition a DataFrame
// MAGIC * Use the following DataFrame operations: `printSchema()`, `select()`, `show()`, `count()`, `groupBy()`, `sum()`, `limit()`, `orderBy()`, `filter()`, `withColumnRenamed()`, `join()`, `withColumn()`
// MAGIC * Write a User Defined Function (UDF)
// MAGIC * Join 2 DataFrames
// MAGIC * Create a Google visualization to understand the clickstream traffic for the "San Diego" article
// MAGIC * Bonus: Explain in DataFrames and SQL
// MAGIC 
// MAGIC Lab idea from <a href="https://ewulczyn.github.io/Wikipedia_Clickstream_Getting_Started/" target="_blank">Ellery Wulczyn</a>

// COMMAND ----------

// MAGIC %md Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

// COMMAND ----------

// MAGIC %md ####![Wikipedia Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_wikipedia_tiny.png) ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Introduction: Wikipedia Clickstream**

// COMMAND ----------

// MAGIC %md The file we are exploring in this lab is the February 2015 English Wikipedia Clickstream data.

// COMMAND ----------

// MAGIC %md ####![Intro to data](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/intro_to_data.png)

// COMMAND ----------

// MAGIC %md How large is the data? Let's use %fs to find out:

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed

// COMMAND ----------

// MAGIC %md A size of 1322171548 bytes means 1.2 GB.

// COMMAND ----------

// MAGIC %md According to Wikimedia: 
// MAGIC 
// MAGIC >"The data contains counts of (referer, resource) pairs extracted from the request logs of English Wikipedia. When a client requests a resource by following a link or performing a search, the URI of the webpage that linked to the resource is included with the request in an HTTP header called the "referer". This data captures 22 million (referer, resource) pairs from a total of 3.2 billion requests collected during the month of February 2015."

// COMMAND ----------

// MAGIC %md Visually, you can imagine a user clicking through different Wikipedia articles to generate the clickstream data:

// COMMAND ----------

// MAGIC %md ####![Clickstream articles](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/clickstream_articles.png)

// COMMAND ----------

// MAGIC %md Use the `sqlContext` to read a tab seperated values file (TSV) of the Clickstream data.

// COMMAND ----------

// Notice that the sqlContext in Databricks is actually a HiveContext
sqlContext

// COMMAND ----------

// MAGIC %md A `HiveContext` includes additional features like the ability to write queries using the more complete HiveQL parser, access to Hive UDFs, and the ability to read data from Hive tables. In general, you should always aim to use the `HiveContext` over the more limited `sqlContext`.

// COMMAND ----------

// MAGIC %md Use the <a href="https://github.com/databricks/spark-csv" target="_blank">Spark CSV Library</a> to parse the tab separated file:

// COMMAND ----------

//Create a DataFrame with the anticipated structure
val clickstreamDF = sqlContext.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("delimiter", "\\t")
  .option("mode", "PERMISSIVE")
  .option("inferSchema", "true")
  .load("dbfs:///databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed")

// COMMAND ----------

// MAGIC %md Note that it took 1 minute to read the 1.2 GB file from S3. The above cell kicked off 2 Spark jobs, the first job has one task and just infers the schema from the file. The 2nd job uses 20 tasks to read the 1.2 GB file in parallel (each task reads about 64 MB of the file). 

// COMMAND ----------

// MAGIC %md The `display()` function shows the DataFrame:

// COMMAND ----------

display(clickstreamDF)

// COMMAND ----------

// MAGIC %md For example, if you go to the "Louden Up Now" Wikipedia article, you will see a link to the !!! music album: <a href="https://en.wikipedia.org/wiki/Louden_Up_Now" target="_blank">Louden_Up_Now</a>

// COMMAND ----------

// MAGIC %md The DataFrame contains 22 million rows with referer/resource pairs:

// COMMAND ----------

// MAGIC %md ####![DF pairs](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/df_pairs.png)

// COMMAND ----------

// MAGIC %md `printSchema()` prints out the schema, the data types and whether a column can be null:

// COMMAND ----------

clickstreamDF.printSchema()

// COMMAND ----------

// MAGIC %md Here is what the 6 columns mean:
// MAGIC 
// MAGIC - `prev_id`: *(note, we already dropped this)* if the referer does not correspond to an article in the main namespace of English Wikipedia, this value will be empty. Otherwise, it contains the unique MediaWiki page ID of the article corresponding to the referer i.e. the previous article the client was on
// MAGIC 
// MAGIC - `curr_id`: *(note, we already dropped this)* the MediaWiki unique page ID of the article the client requested
// MAGIC 
// MAGIC - `prev_title`: the result of mapping the referer URL to the fixed set of values described above
// MAGIC 
// MAGIC - `curr_title`: the title of the article the client requested
// MAGIC 
// MAGIC - `n`: the number of occurrences of the (referer, resource) pair
// MAGIC 
// MAGIC - `type`
// MAGIC   - "link" if the referer and request are both articles and the referer links to the request
// MAGIC   - "redlink" if the referer is an article and links to the request, but the request is not in the production enwiki.page table
// MAGIC   - "other" if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer

// COMMAND ----------

// MAGIC %md The referer (or prev_title) can be one of the following 10 options:

// COMMAND ----------

// MAGIC %md ####![referer](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/referer.png)

// COMMAND ----------

// MAGIC %md `other-empty` is typically HTTPS traffic.

// COMMAND ----------

// MAGIC %md ####![n](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/n.png)

// COMMAND ----------

// MAGIC %md ####![type](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/type.png)

// COMMAND ----------

// MAGIC %md The two id columns (prev_id and curr_id) are not used in this lab, so let's create a new DataFrame without them:

// COMMAND ----------

// Select out just the columns we need for our analysis
val clickstreamNoIDsDF = clickstreamDF.select($"prev_title", $"curr_title", $"n", $"type")

// COMMAND ----------

clickstreamNoIDsDF.show(5)

// COMMAND ----------

// MAGIC %md ####![Databricks Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_databricks_tiny.png) ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Databricks + Spark Architecture**

// COMMAND ----------

// MAGIC %md Before analyzing the Clickstream, let's learn a bit more about the Databricks + Spark architecture. This will help us optimize the DataFrame # of partitions and Spark SQL queries.

// COMMAND ----------

// MAGIC %md Behind the scenes this notebook is connected to a Spark local mode cluster running on a r3.2xlarge EC2 machine in Amazon:

// COMMAND ----------

// MAGIC %md ####![EC2 host](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/ec2machine_w_10contatiners.png)

// COMMAND ----------

// MAGIC %md Each EC2 machine is being shared by 9 other Databricks Community Edition users.
// MAGIC 
// MAGIC The Amazon r3.2xlarge machine has 8 vCPUs and 60 GB of memory. Learn more about <a href="https://aws.amazon.com/ec2/instance-types/" target="_blank">Amazon EC2 Instance Types</a>.

// COMMAND ----------

// MAGIC %md `%sh` is a magic command in Databricks to run shell commands in the container. 
// MAGIC 
// MAGIC Check how much memory is free on the EC2 machine:

// COMMAND ----------

// MAGIC %sh free -mh

// COMMAND ----------

// MAGIC %md The `jps` tool lists the Java Virtual Machines (JVMs) in the container:

// COMMAND ----------

// MAGIC %sh jps

// COMMAND ----------

// MAGIC %md `DriverDaemon` above is the actual 3.7 GB Spark JVM. Ignore `jps` in the output above as it's just the `jps` command running. We'll cover `ChauffeurDaemon` soon.
// MAGIC 
// MAGIC `jps -v` prints verbose details about each JVM:

// COMMAND ----------

// MAGIC %sh jps -v

// COMMAND ----------

// MAGIC %md Above we can see that the DriverDaemon uses 3.7 GB of RAM (-Xms3776m and -Xmx3776m).

// COMMAND ----------

// MAGIC %md The 1 GB Chauffeur JVM is an internal Databricks component that multiplexes commands from different notebooks to the same `DirverDaemon` JVM (Spark local mode cluster):

// COMMAND ----------

// MAGIC %md ####![Chauffeur](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/chauffeur.png)

// COMMAND ----------

// MAGIC %md Chauffeur basically lets you have multiple notebooks attached to the same cluster.

// COMMAND ----------

// MAGIC %md Click on the Executors tab in the Spark UI to see more details about the `DriverDaemon` JVM:

// COMMAND ----------

// MAGIC %md ####![Executors UI](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/local_executors_ui.png)

// COMMAND ----------

// MAGIC %md Note that of the 3.6 GB Driver+Executor JVM, approximately 2.4 GB (in green) is available for memory caching + persistence.

// COMMAND ----------

// MAGIC %md ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Memory, DataFrame partitions and repartitioning**

// COMMAND ----------

// MAGIC %md The 1.2 GB Clickstream file is currently on S3, which means each time you scan through it, your Spark cluster has to read the 1.2 GB of data remotely over the network.

// COMMAND ----------

// MAGIC %md The DataFrame is currently 20 partitions:

// COMMAND ----------

clickstreamNoIDsDF.rdd.partitions.size

// COMMAND ----------

// MAGIC %md We know from our previous labs that just as soon as we do an **orderBy(..)** Spark will use the default partition size of 200.
// MAGIC 
// MAGIC Because we want a factor 3, in this case 6 partitions, we are again going to configure the shuffle partition size before getting started.

// COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "6")

// COMMAND ----------

// MAGIC %md Next we need to actually repartition the DataFrame into 6 partitions (from the initial 20) and cache it with a friendly name:

// COMMAND ----------

val clickstreamNoIDs6partDF = clickstreamNoIDsDF.repartition(6)    // Repartion to 6, a multiple of 3
clickstreamNoIDs6partDF.registerTempTable("Clickstream")           // Register a "temp" table
sqlContext.cacheTable("Clickstream")                               // Cache the table
clickstreamNoIDs6partDF.count                                      // materialize the cache

// COMMAND ----------

// MAGIC %md In the Spark UI, go to the Storage tab, then click on the DataFrame name to see details of the Clickstream DataFrame in memory:

// COMMAND ----------

// MAGIC %md ####![Clickstream in Memory](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/clickstream_in_mem.png)

// COMMAND ----------

// MAGIC %md Notice that each partition (in green) is about 120 MB. An ideal partition size in Spark is about 50 MB - 200 MB.

// COMMAND ----------

// MAGIC %md ####![Clickstream in Memory](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/clickstream_in_mem_diagram.png)

// COMMAND ----------

// MAGIC %md In order to run full scan operations against the Clickstream DataFrame, we will need to use 6 tasks, one per partition. 
// MAGIC 
// MAGIC Since the cluster has only 3 slots, it will run the first 3 tasks and then run 3 more tasks.
// MAGIC 
// MAGIC Let's see this in action with the `count()` action:

// COMMAND ----------

clickstreamNoIDs6partDF.count

// COMMAND ----------

// MAGIC %md Expand the Spark Jobs and Job # above to see that 6 tasks were launched to scan the DataFrame. 

// COMMAND ----------

// MAGIC %md ####![6 Tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/clickstream/6tasks.png)

// COMMAND ----------

// MAGIC %md The first stage was skipped because the DataFrame is already cached in memory and doesn't need to be re-read from disk.

// COMMAND ----------

// MAGIC %md Before continuing, let's rename the DataFrame with a more friendly name (so it's easier to type):

// COMMAND ----------

val clickstreamDF2 = clickstreamNoIDs6partDF

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-1) How much traffic did Google send to the "Apache Spark" article in Feb 2015?

// COMMAND ----------

// MAGIC %md We will need to filter our dataframe where the previous title is Google and the current title is Apache Spark:

// COMMAND ----------

clickstreamDF2.filter($"prev_title" === "other-google" && $"curr_title" === "Apache_Spark").show()

// COMMAND ----------

// MAGIC %md 14,361 visits to "Apache Spark" came from Google.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-2)How many unique articles did Google send traffic to?

// COMMAND ----------

clickstreamDF2.filter($"prev_title" === "other-google").count()

// COMMAND ----------

// MAGIC %md Google sent traffic to about 2.5 million unique articles out of a total of ~5 million articles.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-3) What are the top 10 articles requested from Wikipedia?

// COMMAND ----------

// MAGIC %md We start by grouping by the current title and summing the number of occurrances of the current title:

// COMMAND ----------

display(clickstreamDF2.groupBy("curr_title").sum().limit(10))

// COMMAND ----------

// MAGIC %md To see just the top 10 articles requested, we also need to order by the sum of n column, in descending order.
// MAGIC 
// MAGIC ** Challenge 1:** Can you build upon the code in the cell above to also order by the sum column in descending order, then limit the results to the top ten?
// MAGIC 
// MAGIC Hint, look up the `orderBy()` operation in the <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame" target="_blank">DataFrame API docs</a> via a new browser tab.

// COMMAND ----------

// Type your answer here...


// COMMAND ----------

// MAGIC %md The most requested articles tend to be about media that was popular in February 2015, with a few exceptions.

// COMMAND ----------

// MAGIC %md Spark SQL lets you seemlessly move between DataFrames and SQL. We can run the same query using SQL:

// COMMAND ----------

//First register the table, so we can call it from SQL
clickstreamDF2.registerTempTable("clickstream")

// COMMAND ----------

// MAGIC %md Do a simple "Select all" query from the `clickstream` table to make sure it's working:

// COMMAND ----------

// MAGIC %sql SELECT * FROM clickstream LIMIT 6;

// COMMAND ----------

// MAGIC %md Now we can translate our DataFrames query to SQL:

// COMMAND ----------

// MAGIC %sql SELECT curr_title, SUM(n) AS top_articles FROM clickstream WHERE curr_title != 'Main_Page' GROUP BY curr_title ORDER BY top_articles DESC LIMIT 10;

// COMMAND ----------

// MAGIC %md SQL also has some handy commands like `DESC` (describe) to see the schema + data types for the table:

// COMMAND ----------

// MAGIC %sql DESC clickstream;

// COMMAND ----------

// MAGIC %md You can use the `SHOW FUNCTIONS` command to see what functions are supported by Spark SQL:

// COMMAND ----------

// MAGIC %sql SHOW FUNCTIONS;

// COMMAND ----------

// MAGIC %md `EXPLAIN` can be used to understand the Physical Plan of the SQL statement:

// COMMAND ----------

// MAGIC %sql EXPLAIN SELECT curr_title, SUM(n) AS top_articles FROM clickstream WHERE curr_title != 'Main_Page' GROUP BY curr_title ORDER BY top_articles DESC LIMIT 10;

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-4) Who sent the most traffic to Wikipedia in Feb 2015? So, who were the top referers to Wikipedia?

// COMMAND ----------

clickstreamDF2
  .groupBy("prev_title")
  .sum()
  .orderBy($"sum(n)".desc)
  .show(10)

// COMMAND ----------

// MAGIC %md The top referer by a large margin is Google, which sent about 1.5 billion clicks to Wikipedia. Next comes refererless traffic (usually clients using HTTPS). The third largest sender of traffic to English Wikipedia are Wikipedia pages that are not in the main namespace (ns = 0) of English Wikipedia. Learn about the Wikipedia namespaces here:
// MAGIC <a href="https://en.wikipedia.org/wiki/Wikipedia:Project_namespace" target="_blank">Wikipedia:Project_namespace</a>
// MAGIC 
// MAGIC Also, note that Twitter sends 10x more requests to Wikipedia than Facebook.

// COMMAND ----------

// MAGIC %md Let's generate a bar graph visualization for this using the display command and changing the Plot Options:

// COMMAND ----------

display(clickstreamDF2.groupBy("prev_title").sum().orderBy($"sum(n)".desc).limit(10))

// COMMAND ----------

// MAGIC %md To generate a bar graph, click the Bar chart icon:

// COMMAND ----------

// MAGIC %md 
// MAGIC #![Bar Chart](http://i.imgur.com/XerPxW2.jpg)

// COMMAND ----------

// MAGIC %md Then click Plot Options and rearrange the Keys and Values as soon in the screenshot below:

// COMMAND ----------

// MAGIC %md 
// MAGIC #![Plot Options](http://i.imgur.com/dA4nux6.jpg)

// COMMAND ----------

// MAGIC %md 
// MAGIC #![Customize Plot](http://i.imgur.com/AaF9vSo.jpg)

// COMMAND ----------

// MAGIC %md How many total requests were there for English Wikipedia pages in Feb 2015?

// COMMAND ----------

// Import the sql functions package, which includes statistical functions like sum, max, min, avg, etc.
import org.apache.spark.sql.functions._

// COMMAND ----------

clickstreamDF2.select(sum($"n")).show()

// COMMAND ----------

// MAGIC %md So 3.2 billion requests were made to English Wikipedia in Feb 2015.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-5) What percentage of the traffic Wikipedia received came from other English Wikipedia pages? 
// MAGIC 
// MAGIC So, how many of the clicks came from other English Wikipedia pages?

// COMMAND ----------

// MAGIC %md Perhaps Google sent 1.5 billion clicks, but how many clicks did Wikipedia generate from internal traffic... people clicking from one Article into another Article and so on?

// COMMAND ----------

// MAGIC %md ** Challenge 2:** Can you answer this question using DataFrames? Hint: Filter out all of the rows where the prev_title is google, twitter, facebook, etc and then sum up the n column.

// COMMAND ----------

// Type your answer here...


// COMMAND ----------

// MAGIC %md Only ~1.1 billion clicks were generated by people browsing through Wikipedia by clicking from one article to another.
// MAGIC 
// MAGIC Google wins!

// COMMAND ----------

// MAGIC %md Did you notice in the job above that Spark pipelined all of the filters into the first Stage?

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-6) What were the top 5 trending articles on Twitter?

// COMMAND ----------

// MAGIC %md ** Challenge 3:** Can you answer this question using DataFrames?

// COMMAND ----------

// Type your answer here...


// COMMAND ----------

// MAGIC %md ** Challenge 4:** Try re-writing the query above using SQL:

// COMMAND ----------

// Type your answer here... (tip: you must remove this comment)


// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-7) What are the most requested missing pages?  
// MAGIC 
// MAGIC (These are the articles that someone should create on Wikipedia!)

// COMMAND ----------

// MAGIC %md The type column of our table has 3 possible values:

// COMMAND ----------

// MAGIC %sql SELECT DISTINCT type FROM clickstream;

// COMMAND ----------

// MAGIC %md These are described as:
// MAGIC   - **link** - if the referer and request are both articles and the referer links to the request
// MAGIC   - **redlink** - if the referer is an article and links to the request, but the request is not in the production enwiki.page table
// MAGIC   - **other** - if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer

// COMMAND ----------

// MAGIC %md Redlinks are links to a Wikipedia page that does not exist, either because it has been deleted, or because the author is anticipating the creation of the page. Seeing which redlinks are the most viewed is interesting because it gives some indication about demand for missing content.
// MAGIC 
// MAGIC Let's find the most popular redlinks:

// COMMAND ----------

display(clickstreamDF2.filter("type = 'redlink'").groupBy("curr_title").sum().orderBy($"sum(n)".desc).limit(10))

// COMMAND ----------

// MAGIC %md Indeed there doesn't appear to be an article on the Russian actress <a href="https://en.wikipedia.org/wiki/Anna_Lezhneva" target="_blank">Anna Lezhneva</a> on Wikipedia. Maybe you should create it!
// MAGIC 
// MAGIC Note that if you clicked on the link for Anna Lezhneva in this cell, then you registered another Redlink for her article.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-8) What does the traffic inflow vs outflow look like for the most requested pages?

// COMMAND ----------

// MAGIC %md Wikipedia users get to their desired article by either searching for the article in a search engine or navigating from one Wikipedia article to another by following a link. For example, depending on which technique a user used to get to his desired article of **San Diego**, the (`prev_title`, `curr_title`) tuples would look like:
// MAGIC - (`other-google`, `San Diego`)
// MAGIC - (`California`, `San Diego`)
// MAGIC 
// MAGIC Lets look at the ratio of incoming to outgoing links for the most requested pages.

// COMMAND ----------

// MAGIC %md First, find the pageviews per article:

// COMMAND ----------

val pageviewsPerArticleDF = clickstreamDF2
  .groupBy("curr_title")
  .sum()
  .withColumnRenamed("sum(n)", "in_count")
  .cache()

pageviewsPerArticleDF.show(10)

// COMMAND ----------

// MAGIC %md Above we can see that the `.17_Remington` article on Wikipedia in Feb 2015, got 2,143 views.

// COMMAND ----------

// MAGIC %md Then, find the link clicks per article:

// COMMAND ----------

val linkclicksPerArticleDF = clickstreamDF2
  .groupBy("prev_title")
  .sum()
  .withColumnRenamed("sum(n)", "out_count")
  .cache()

linkclicksPerArticleDF.show(10)

// COMMAND ----------

// MAGIC %md So, when people went to the `David_Janson` article on Wikipedia in Feb 2015, 340 times they clicked on a link in that article to go to a next article. 

// COMMAND ----------

// MAGIC %md Join the two DataFrames we just created to get a wholistic picture:

// COMMAND ----------

val in_outDF = pageviewsPerArticleDF.join(linkclicksPerArticleDF, ($"curr_title" === $"prev_title")).orderBy($"in_count".desc)

in_outDF.show(10)

// COMMAND ----------

// MAGIC %md The `curr_title` and `prev_title` above are the same, so we can just display one of them in the future. Next, add a new `ratio` column to easily see whether there is more `in_count` or `out_count` for an article:

// COMMAND ----------

val in_out_ratioDF = in_outDF.withColumn("ratio", $"out_count" / $"in_count").cache()

in_out_ratioDF.select($"curr_title", $"in_count", $"out_count", $"ratio").show(5)

// COMMAND ----------

// MAGIC %md We can see above that when clients went to the **Alive** article, almost nobody clicked any links in the article to go on to another article.
// MAGIC 
// MAGIC But 49% of people who visited the **Fifty Shades of Grey** article clicked on a link in the article and continued to browse Wikipedia.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-8)  What does the traffic flow pattern look like for the "Apache_Spark" article? Create a visualization for this. 

// COMMAND ----------

in_out_ratioDF.filter("curr_title = 'Apache_Spark'").show()

// COMMAND ----------

// MAGIC %md Hmm, so about 18% of clients who visit the "Apache_Spark" page, click on through to another article.

// COMMAND ----------

// MAGIC %md Which referrers send the most traffic to the Spark article?

// COMMAND ----------

// MAGIC %sql SELECT * FROM clickstream WHERE curr_title LIKE 'Apache_Spark' ORDER BY n DESC LIMIT 10;

// COMMAND ----------

// MAGIC %md Here's the same query using DataFrames and `show()`:

// COMMAND ----------

clickstreamDF2.filter($"curr_title".rlike("""^Apache_Spark$""")).orderBy($"n".desc).show(10)

// COMMAND ----------

// MAGIC %md ** Challenge 5:** Which future articles does the "Apache_Spark" article send most traffic onward to? Try writing this query using the DataFrames API:

// COMMAND ----------

// Type your answer here...


// COMMAND ----------

// MAGIC %md Above we can see the topics most people are interested in, when they get to the "Apache_Spark" article.

// COMMAND ----------

// MAGIC %md Finally, we'll use a Google Visualization library to create a Sankey diagram. Sankey diagrams are a flow diagram, in which the width of the arrows are shown proportionally to the flow quantify traffic:

// COMMAND ----------

// Note you may need to disable your privacy browser extensions to make this work (especially Privacy Badger)

var content = """
<!DOCTYPE html>
<body>
<script type="text/javascript"
           src="https://www.google.com/jsapi?autoload={'modules':[{'name':'visualization','version':'1.1','packages':['sankey']}]}">
</script>

<div id="sankey_multiple" style="width: 800px; height: 640px;"></div>

<script type="text/javascript">
google.setOnLoadCallback(drawChart);
   function drawChart() {
    var data = new google.visualization.DataTable();
    data.addColumn('string', 'From');
    data.addColumn('string', 'To');
    data.addColumn('number', 'Weight');
    data.addRows([
"""
  
clickstreamDF2
  .filter( ($"curr_title".rlike("""^Apache_Spark$""") and $"prev_title".rlike("""^other-.*$""")) or ($"prev_title".rlike("""^Apache_Spark$""") and $"n" > 100) )
  .orderBy($"n")
  .collect().foreach(x => { content += s"\n['${x.get(0)}', '${x.get(1)}', ${x.get(2)}]," })

content += """
    ]);
    // Set chart options
    var options = {
      width: 600,
      sankey: {
        link: { color: { fill: '#grey', fillOpacity: 0.3 } },
        node: { color: { fill: '#a61d4c' },
                label: { color: 'black' } },
      }
    };
    // Instantiate and draw our chart, passing in some options.
    var chart = new google.visualization.Sankey(document.getElementById('sankey_multiple'));
    chart.draw(data, options);
   }
</script>
  </body>
</html>"""

displayHTML(content)

// COMMAND ----------

// MAGIC %md The chart above shows how people get to a Wikipedia article and what articles they click on next.
// MAGIC 
// MAGIC This diagram shows incoming and outgoing traffic to the "Apache_Spark" article. We can see that most people found the "San Diego" page through Google search and only a small fraction of the readers went on to another article

// COMMAND ----------

// MAGIC %md Note that it is also possible to programmatically add in the values in the HTML, so you don't have to hand-code it. But to keep things simple, we've hand coded it above. To learn how to do it programatically, check out this notebook: Databricks Guide / Visualizations / HTML, D3 and SVG.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Bonus:
// MAGIC ** Learning about Explain to understand Catalyst internals **

// COMMAND ----------

// MAGIC %md ![Catalyst Optimizer](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/catalyst.png)

// COMMAND ----------

// MAGIC %md The `explain()` method can be called on a DataFrame to understand its physical plan:

// COMMAND ----------

in_out_ratioDF.explain()

// COMMAND ----------

// MAGIC %md You can also pass in `true` to see the logical & physical plans:

// COMMAND ----------

in_out_ratioDF.explain(true)
