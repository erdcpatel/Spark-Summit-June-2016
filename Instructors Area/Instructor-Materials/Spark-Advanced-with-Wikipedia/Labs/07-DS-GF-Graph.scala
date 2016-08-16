// Databricks notebook source exported at Wed, 1 Jun 2016 22:43:28 UTC
// MAGIC %md ### Start with the Bonus Graph D3js Notebook, then return here...
// MAGIC 
// MAGIC (Note, there is only a ReadMe version of this lab. This will move us along fast into ML...)

// COMMAND ----------

// MAGIC %md ![Wikipedia/Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark.png)  
// MAGIC 
// MAGIC #### Analyze Clickstream using GraphFrames
// MAGIC 
// MAGIC **Objective:**
// MAGIC Quick tour of the upcoming GraphFrames API
// MAGIC 
// MAGIC **Time to Complete:**
// MAGIC 10 mins
// MAGIC 
// MAGIC 
// MAGIC People icons from <a href="http://www.icons-land.com" target="_blank">Icons Land</a>

// COMMAND ----------

// MAGIC %md Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

// COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "60")

// COMMAND ----------

// MAGIC %md ####![Wikipedia Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_wikipedia_tiny.png) ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Introduction: Graph Fundamentals **

// COMMAND ----------

// MAGIC %md Graphs are made of vertices (the nodes) and edges (the connections):

// COMMAND ----------

// MAGIC %md ![Spark and Kevin](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/spark_hadoop.png)

// COMMAND ----------

// MAGIC %md Each vertex has an ID (the unique article ID) and each edge has a label (number of clicks and connection type):

// COMMAND ----------

// MAGIC %md ![Spark and Hadoop](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/spark_hadoop_detailed.png)

// COMMAND ----------

// MAGIC %md A GraphFrame is created by combining a vertices dataframe with a edges dataframe:

// COMMAND ----------

// MAGIC %md ![Spark GraphFrame](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/creating_gf.png)

// COMMAND ----------

// MAGIC %md Read the vertices dataframe and cache it:

// COMMAND ----------

val verticesDF = sqlContext
  .read
  .parquet("/mnt/wikipedia-readonly/gx_clickstream/vertices")
  .cache

// COMMAND ----------

verticesDF.show(5)

// COMMAND ----------

// MAGIC %md How many vertices are in the DataFrame?

// COMMAND ----------

// this cell will also materialize the cache
verticesDF.count

// COMMAND ----------

// MAGIC %md Read the edges dataframe and cache it:

// COMMAND ----------

val edgesDF = sqlContext
  .read
  .parquet("/mnt/wikipedia-readonly/gx_clickstream/edges")
  .cache

// COMMAND ----------

edgesDF.show(5)

// COMMAND ----------

// MAGIC %md How many edges are in the DataFrame?

// COMMAND ----------

// this cell will also materialize the cache
// 85 sec to run
edgesDF.count

// COMMAND ----------

// MAGIC %md Combine the verticesDF and edgesDF into a GraphFrame:

// COMMAND ----------

// MAGIC %md Before we can do anything, we need the <a href="https://github.com/graphframes/graphframes.github.io" target="_blank">Graph Frames API</a>.
// MAGIC 0. Open your workspace
// MAGIC 0. Right-click > Create > Library
// MAGIC 0. Change the source to **Maven Coordinate**
// MAGIC 0. Click **Search SparkPackages and Maven Central**
// MAGIC 0. Search for **graphframes**
// MAGIC 0. Select the correct release
// MAGIC 0. Click **Select**
// MAGIC 0. Click **Create Library**
// MAGIC 0. Click **Attach automatically to all clusters.**
// MAGIC 0. Restart your cluster

// COMMAND ----------

import org.graphframes._


// COMMAND ----------

val clickstreamGRAPH = GraphFrame(verticesDF, edgesDF)

// COMMAND ----------

// MAGIC %md A vertex can point to many other vertices:

// COMMAND ----------

// MAGIC %md ![Spark and Kevin](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/spark_to_3ver.png)

// COMMAND ----------

// MAGIC %md Above we see that the Spark article sent 808 clicks to the Hadoop article, 643 clicks to Matei and 322 to Mesos.
// MAGIC 
// MAGIC Here's the corresponding DataFrames query:

// COMMAND ----------

clickstreamGRAPH.edges.filter($"src_title" === "Apache_Spark").orderBy($"n".desc).show(8)

// COMMAND ----------

// MAGIC %md Other vertices may also refer traffic to the Spark article:

// COMMAND ----------

// MAGIC %md ![Spark Graph](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/spark_graph.png)

// COMMAND ----------

// MAGIC %md Above and below, notice that Google sent 14,361 clicks to Apache Spark. The Hadoop article sent 901 clicks to Spark, while Spark sent 808 clicks to Hadoop.

// COMMAND ----------

clickstreamGRAPH.edges.filter($"dst_title" === "Apache_Spark").orderBy($"n".desc).show(8)

// COMMAND ----------

// MAGIC %md More accurately, there is only one Hadoop article. It has 2 unidirectional edges to the Spark article:

// COMMAND ----------

// MAGIC %md ![Spark Graph Fixed](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/spark_graph_fixed.png)

// COMMAND ----------

// MAGIC %md ####![Wikipedia Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_wikipedia_tiny.png) **Graphs are Everywhere..**

// COMMAND ----------

// MAGIC %md Graphs appear all over the place. Sometimes they're obvious to spot in the data, other times they're hidden. Consider a few examples:

// COMMAND ----------

// MAGIC %md The following diagram shows 20 Wikipedians editing 4 articles:

// COMMAND ----------

// MAGIC %md ![Wikipedians Edits Graph title](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/wikipedians_edits_graph_title.png)
// MAGIC 
// MAGIC ![Wikipedians Edits Graph](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/wikipedians_edits_graph.png)

// COMMAND ----------

// MAGIC %md The Wikipedians above consist of:
// MAGIC - 5 sports players
// MAGIC - 5 pilots
// MAGIC - 4 artists
// MAGIC - 4 medical staff
// MAGIC - 1 firefigher
// MAGIC - 1 pizza deliverer 
// MAGIC 
// MAGIC When looking at the editors of the <a href="https://en.wikipedia.org/wiki/2016_Summer_Olympics" target="_blank">Rio 2016 Summer Olympics article</a>, notice that the swimmer, judo and soccer players have thicker orange lines than the 2 american football players. The thickness of the line idicates how many edits each person did. Since the Summer Olympics don't have american football, perhaps football players have less interest in the Olympics?
// MAGIC 
// MAGIC Among the artists, the 2 actors and movie producer did the most edits to the <a href="https://en.wikipedia.org/wiki/Academy_Awards" target="_blank">Academy Awards article</a>, while the musician and pizza deliverer contributed just a little.
// MAGIC 
// MAGIC The violin player seems interested in both the <a href="https://en.wikipedia.org/wiki/Academy_Awards" target="_blank">Academy Awards</a> and <a href="https://en.wikipedia.org/wiki/CRISPR" target="_blank">CRISPR</a>.
// MAGIC 
// MAGIC Mostly the medical staff (doctors, nurse, pharmacist) are editing the <a href="https://en.wikipedia.org/wiki/CRISPR" target="_blank">CRISPR article</a>. CRISPR is a new genome/DNA editing tool that could transform the field of biology and genetic engineering. 
// MAGIC 
// MAGIC The 2 commercial pilots look like they did more edits to the <a href="https://en.wikipedia.org/wiki/Boeing_777X" target="_blank">Boeing 777x article</a>, than the 3 military pilots... probably because the $400 million dollar 777x plane, which is planned for release in 2020, is going to be a commercial airplane.
// MAGIC 
// MAGIC The pizza delivery woman seems to have pretty diverse interests.

// COMMAND ----------

// MAGIC %md All of the links in the Wikipedia articles also make a graph:

// COMMAND ----------

// MAGIC %md 
// MAGIC ![En Wikipedia Graph Title](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/en_wiki_graph_title.png)
// MAGIC 
// MAGIC ![En Wikipedia Spark Graph](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/en_wiki_graph_spark.png)

// COMMAND ----------

// MAGIC %md Can you guess what this graph shows?

// COMMAND ----------

// MAGIC %md ![Sock graph](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/sock_graph.png)

// COMMAND ----------

// MAGIC %md The above graph was created by the Wikimedia Foundation. Following an investigation named <a href="https://en.wikipedia.org/wiki/Wikipedia:Long-term_abuse/Orangemoody" target="_blank">Orangemoody"</a>, WMF identified 381 malicious Wikipedia editor IPs/accounts who were creating promotional articles, inserting promotional external links and doing disruptive edits for profit.
// MAGIC 
// MAGIC Yellow bubbles represent IP addresses of anon editors, and green bubbles represent accounts with usernames.
// MAGIC 
// MAGIC All 381 accounts were blocked from editing Wikipedia.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Graph Algorithms: inDegree and outDegree**

// COMMAND ----------

// MAGIC %md The `inDegree` of a vertex shows how many edges point into it:

// COMMAND ----------

// MAGIC %md ![inDegree](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/indegree.png)

// COMMAND ----------

// MAGIC %md To calculate the `inDegree` of the Apache Spark article, start by getting the article id:

// COMMAND ----------

clickstreamGRAPH.vertices.filter($"id_title" === "Apache_Spark").show

// COMMAND ----------

// MAGIC %md Then get the `inDegree` of all the vertices and filter for just the Apache Spark article:

// COMMAND ----------

clickstreamGRAPH.inDegrees.filter($"id" === 42164234).show()

// COMMAND ----------

// MAGIC %md So, traffic from 33 other unique articles clicked into the Apache Spark article in Feb 2015.

// COMMAND ----------

// MAGIC %md The `outDegree` of a vertex shows how many edges point out of it:

// COMMAND ----------

// MAGIC %md ![outDegree](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/outdegree.png)

// COMMAND ----------

// MAGIC %md Query the `outDegree` of all the vertices and filter for just the Apache Spark article:

// COMMAND ----------

clickstreamGRAPH.outDegrees.filter($"id" === 42164234).show()

// COMMAND ----------

// MAGIC %md The Apache Spark article sent traffic to 30 other unqiue articles.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Graph Algorithms: SubGraph**

// COMMAND ----------

printf("%,d vertices\n", clickstreamGRAPH.vertices.count)

val verticesSubgraphDF = clickstreamGRAPH.vertices.filter($"id_title" !== "Main_Page")
val count = verticesSubgraphDF.cache.count // cache and materialize

printf("%,d sub-vertices\n\n", count)

// COMMAND ----------

verticesSubgraphDF.show(4)

// COMMAND ----------

printf("%,d edges\n", edgesDF.count)

val edgesSubgraphDF = edgesDF.filter("n > '300'").filter($"src" !== 15580374).filter($"dst" !== 15580374)
val count = edgesSubgraphDF.cache.count // cache and materialize

printf("%,d sub-edges\n\n", count)

// COMMAND ----------

edgesSubgraphDF.show(4)

// COMMAND ----------

val edgesSubgraphDF2 = edgesDF
.filter($"src" !== 15580374) //remove main page
.filter($"dst" !== 15580374)
.filter($"src_title" !== "other-google")
.filter($"src_title" !== "other-wikipedia")
.filter($"src_title" !== "other-empty")
.filter($"src_title" !== "other-bing")
.filter($"src_title" !== "other-twitter")
.filter($"src_title" !== "other-facebook")
.filter($"src_title" !== "other-other")
.filter($"src_title" !== "other-yahoo")
.filter($"src_title" !== "other-internal")

printf("%,d sub-edges\n\n", edgesSubgraphDF2.count)

// COMMAND ----------

val clickstreamSubGRAPH = GraphFrame(verticesSubgraphDF, edgesSubgraphDF2)

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Graph Algorithm: Shortest Path**

// COMMAND ----------

// MAGIC %md ![Shortest Path Question](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/graph/shortest_path_q.png)

// COMMAND ----------

clickstreamGRAPH.vertices.filter($"id_title" === "Apache_Spark").show

// COMMAND ----------

clickstreamGRAPH.vertices.filter($"id_title" === "Kevin_Bacon").show

// COMMAND ----------

//spark
clickstreamSubGRAPH.outDegrees.filter($"id" === 42164234).show()

// COMMAND ----------

//kevin
clickstreamSubGRAPH.inDegrees.filter($"id" === 16827).show()

// COMMAND ----------

// MAGIC %md Breadth-first search (BFS) finds the shortest path(s) from one vertex to another vertex:

// COMMAND ----------

// 7-8 mins to run
val shortestPathDF =  clickstreamSubGRAPH.bfs.fromExpr("id = 42164234").toExpr("id = 16827").run() 

// COMMAND ----------

//How many results did we get?

// 4 mins to run
shortestPathDF.cache.count

// COMMAND ----------

display(shortestPathDF)

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Graph Algorithm: PageRank**

// COMMAND ----------

// 13 mins to run
val prGRAPH = clickstreamGRAPH.pageRank.maxIter(3).run()  //621.45s to run 

// COMMAND ----------

prGRAPH.vertices.show(10)

// COMMAND ----------

display(prGRAPH.vertices.orderBy($"pagerank".desc).limit(30))
