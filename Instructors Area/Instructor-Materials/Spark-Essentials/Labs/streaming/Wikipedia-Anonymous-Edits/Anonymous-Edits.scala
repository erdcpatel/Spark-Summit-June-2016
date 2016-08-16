// Databricks notebook source exported at Fri, 15 Apr 2016 17:56:34 UTC
// MAGIC %md
// MAGIC # Spark Streaming and Wikipedia ![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC ### Time to complete: 20-30 minutes
// MAGIC 
// MAGIC #### Business Questions:
// MAGIC 
// MAGIC * Question 1: Where are the recent anonymous English Wikipedia editors located in the world?
// MAGIC * Question 2: Are edits being made to the English Wikipedia from non-English speaking countries? If so, where?
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Increased understanding of the Spark Streaming UI.
// MAGIC * Comparison between using Kafka Direct Connect vs. connecting to a TCP socket.
// MAGIC * How to visualize data in a Databricks notebook.

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Preparation
// MAGIC 
// MAGIC In this lab, we're going to analyze some streaming Wikipedia edit data.
// MAGIC 
// MAGIC First, let's get some imports out of the way.

// COMMAND ----------

import org.apache.spark.sql.SaveMode

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import _root_.kafka.serializer.StringDecoder
import scala.util.Random

// COMMAND ----------

// MAGIC %md Now, some configuration constants.

// COMMAND ----------

val ID                   = java.util.UUID.randomUUID().toString.replace("-", "_")
val StreamingServerHost  = "ec2-54-213-33-240.us-west-2.compute.amazonaws.com"
val StreamingServerPort  = 9002
val KafkaServerHost      = StreamingServerHost
val KafkaServerPort      = 9092
val TableName            = s"""Edits_${ID.replace("-", "_")}"""
val BatchInterval        = Seconds(15)

// COMMAND ----------

// MAGIC %md We're going to be buffering our data in a Hive table, so let's make sure a fresh one exists.

// COMMAND ----------

sqlContext.sql(s"DROP TABLE IF EXISTS $TableName")
sqlContext.sql(s"""|CREATE TABLE $TableName (wikipedia STRING, user STRING, isAnonymous BOOLEAN, 
                   |pageURL STRING, page STRING, timestamp TIMESTAMP, isRobot BOOLEAN, namespace STRING,
                   |geoCity STRING, geoStateProvince STRING, 
                   |geoCountry STRING, geoCountryCode2 STRING, geoCountryCode3 STRING)""".stripMargin)

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Streaming Logic
// MAGIC 
// MAGIC The following couple of cells contain our Spark Streaming logic.
// MAGIC 
// MAGIC The Streaming code:
// MAGIC 
// MAGIC * Receives each Streaming-produced RDD, which contains individual change records as JSON objects (strings)
// MAGIC * Converts the RDD of string JSON records into a DataFrame
// MAGIC * Selects only the columns we really care to process
// MAGIC * Saves the changes to a Hive table
// MAGIC 
// MAGIC ### The fields we're keeping
// MAGIC 
// MAGIC Here are the fields we're keeping from the incoming data:
// MAGIC 
// MAGIC * `geocoding` (`OBJECT`): Added by the server, this field contains IP address geocoding information for anonymous edits.
// MAGIC   We're keeping some of it.
// MAGIC * `isAnonymous` (`BOOLEAN`): Whether or not the change was made by an anonymous user.
// MAGIC * `isRobot` (`BOOLEAN`): Whether the edit was made by a robot (`true`) or a human (`false`).
// MAGIC * `namespace` (`STRING`): The page's namespace. See <https://en.wikipedia.org/wiki/Wikipedia:Namespace> Some examples:
// MAGIC     * "special": This is a special page (i.e., its name begins with "Special:")
// MAGIC     * "user": the page is a user's home page.
// MAGIC     * "talk": a discussion page about a particular real page
// MAGIC     * "category": a page about a category
// MAGIC     * "file": a page about an uploaded file
// MAGIC     * "media": a page about some kind of media
// MAGIC     * "template": a template page
// MAGIC * `pageURL` (`STRING`): The URL of the page that was edited.
// MAGIC * `page`: (`STRING`): The printable name of the page that was edited
// MAGIC * `timestamp` (`TIMESTAMP`): The time the edit occurred, as a `java.sql.Timestamp`.
// MAGIC * `user` (`STRING`): The user who made the edit or, if the edit is anonymous, the IP address associated with the anonymous editor.
// MAGIC 
// MAGIC Here are the fields we're ignoring:
// MAGIC 
// MAGIC * `channel` (`STRING`): The Wikipedia IRC channel, e.g., "#en.wikipedia"
// MAGIC * `comment` (`STRING`): The comment associated with the change (i.e., the commit message).
// MAGIC * `delta` (`INT`): The number of lines changes, deleted, and/or added.
// MAGIC * `flag`: Various flags about the edit.
// MAGIC     * "m" means the user marked the edit as "minor".
// MAGIC     * "N" means the page is new.
// MAGIC     * "b" means the page was edited by a 'bot.
// MAGIC     * "!" means the page is unpatrolled. (See below.)
// MAGIC * `isNewPage` (`BOOLEAN`): Whether or not the edit created a new page.
// MAGIC * `isUnpatrolled` (`BOOLEAN`): Whether or not the article is patrolled. See <https://en.wikipedia.org/wiki/Wikipedia:New_pages_patrol/Unpatrolled_articles>
// MAGIC * `url` (`STRING`): The URL of the edit diff.
// MAGIC * `userUrl` (`STRING`): The Wikipedia profile page of the user, if the edit is not anonymous.
// MAGIC * `wikipedia` (`STRING`): The readable name of the Wikipedia that was edited (e.g., "English Wikipedia").
// MAGIC * `wikipediaURL` (`STRING`): The URL of the Wikipedia edition containing the article.

// COMMAND ----------

// MAGIC %md ### TCP version
// MAGIC This version of our streaming logic connects directly to the TCP socket of the streaming server. It uses a receiver thread.

// COMMAND ----------

def createContextTCP(): StreamingContext = {
  val ssc = new StreamingContext(sc, BatchInterval)
  println(s"Connecting to $StreamingServerHost, port $StreamingServerPort")
  val stream = ssc.socketTextStream(StreamingServerHost, StreamingServerPort)

  stream.foreachRDD { rdd => 
    import org.apache.spark.sql._
    if (! rdd.isEmpty) {
      val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      // Convert the RDD to a DataFrame
      val df = sqlContext.read.json(rdd)
                         .select($"wikipedia", $"user", $"isAnonymous", $"pageURL",
                                 $"page", $"timestamp", $"isRobot", $"namespace",
                                 $"geocoding.city".as("geoCity"),
                                 $"geocoding.stateProvince".as("geoStateProvince"),
                                 $"geocoding.country".as("geoCountry"),
                                 $"geocoding.countryCode2".as("geoCountryCode2"),
                                 $"geocoding.countryCode3".as("geoCountryCode3"))
                         .coalesce(1)
      // Append this streamed RDD to our persistent table.
      df.write.mode(SaveMode.Append).saveAsTable(TableName)
    } 
  }

  println("Starting context.")
  ssc.start()
  ssc
}

// COMMAND ----------

// MAGIC %md ### Kafka Direct version
// MAGIC This version of our streaming logic connects to a Kafka server. The TCP server feeds the Kafka server (one topic per Wikipedia language), so we're getting the same data. But this version is:
// MAGIC 
// MAGIC * more efficient, because we're not wasting a receiver thread
// MAGIC * more resilient, because Kafka is buffering the data

// COMMAND ----------

def createContextKafka(): StreamingContext = {
  val ssc = new StreamingContext(sc, BatchInterval)
  println(s"Connecting to Kafka on $KafkaServerHost, port $KafkaServerPort")
  val kafkaParams = Map("metadata.broker.list" -> s"$KafkaServerHost:$KafkaServerPort")
  val topics = Set("en", "fr", "de", "it", "ja", "es", "eo", "nn", "nl", "pl", "sv")
  val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  // The stream is a DStream[(String, String)], consisting of a Kafka topic
  // and a JSON string. We don't need the topic, since it's also part of the
  // JSON.
  val stream2 = stream.map { case (topic, json) => json }    
  // Now, process the resulting JSON.
  stream2.foreachRDD { rdd =>
    import org.apache.spark.sql._
    if (! rdd.isEmpty) {
      val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      // Convert the RDD to a DataFrame
      val df = sqlContext.read.json(rdd)
                         .select($"wikipedia", $"user", $"isAnonymous", $"pageURL",
                                 $"page", $"timestamp", $"isRobot", $"namespace",
                                 $"geocoding.city".as("geoCity"),
                                 $"geocoding.stateProvince".as("geoStateProvince"),
                                 $"geocoding.country".as("geoCountry"),
                                 $"geocoding.countryCode2".as("geoCountryCode2"),
                                 $"geocoding.countryCode3".as("geoCountryCode3"))
                         .coalesce(1)
      // Append this streamed RDD to our persistent table.
      df.write.mode(SaveMode.Append).saveAsTable(TableName)
    } 
  }
  
  println("Starting context.")
  ssc.start()
  ssc
}

// COMMAND ----------

// MAGIC %md Choose which streaming approach we want to use (TCP vs. Kafka)

// COMMAND ----------

val createContext = createContextKafka _
//val createContext = createContextTCP _


// COMMAND ----------

// MAGIC %md Here are some convenience functions to start and stop the streaming context.

// COMMAND ----------

def stop(): Unit = {
  val sscOpt = StreamingContext.getActive // returns Option[StreamingContext]
  if (sscOpt.isDefined) {
    val ssc = sscOpt.get // get the StreamingContext
    ssc.stop(stopSparkContext=false)
    println("Stopped running streaming context.")
  }
}

/** Convenience function to start our stream.
  */
def start() = {
  StreamingContext.getActiveOrCreate(creatingFunc = createContext)
}


// COMMAND ----------

// MAGIC %md Okay, let's fire up the streaming context. We're going to let it run for awhile.

// COMMAND ----------

start()

// COMMAND ----------

// MAGIC %md If you want to stop the streaming context, uncomment the code in the following cell and run it.

// COMMAND ----------

//stop()

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Data Analysis

// COMMAND ----------

// MAGIC %md After about 15 seconds, run the following two cells to verify that data is coming in.

// COMMAND ----------

val df = sqlContext.table(TableName)

// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %md Let's take a quick glance at the data.

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md There are some special pages in there, like "wiki/User:Foo", "wiki/Special:Foo", "wiki/Talk:SomeTopic", etc., that aren't of interest to us. We can filter them out. Let's use the "namespace" field to extract only the articles.

// COMMAND ----------

val cleanedDF = df.filter($"namespace" === "article")

// COMMAND ----------

cleanedDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC How many distinct edits do we have per language?

// COMMAND ----------

display(cleanedDF.select($"wikipedia").groupBy($"wikipedia").count())

// COMMAND ----------

// MAGIC %md
// MAGIC Let's winnow it down to English Wikipedia edits.

// COMMAND ----------

val enDF = cleanedDF.filter($"wikipedia" === "en")

// COMMAND ----------

display(enDF.select($"wikipedia").distinct)

// COMMAND ----------

// MAGIC %md
// MAGIC ## A Bit of Visualization

// COMMAND ----------

// MAGIC %md
// MAGIC How many anonymous edits vs. authenticated edits are there? Let's visualize it.
// MAGIC 
// MAGIC For future reference, here's how to configure the pie chart. Select the pie chart, then click on the Plot Options button, and configure the resulting popup as shown here. It's important to select SUM as the aggregation method.
// MAGIC 
// MAGIC <img src="http://i.imgur.com/LkgYHZF.png" style="height: 500px"/>

// COMMAND ----------

display(enDF.groupBy($"isAnonymous").count())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Let's use the Databricks pie chart to graph anonymous vs. authenticated vs. robot edits, so we can visualize the percentage of edits made by robots, by authenticated users, and by anonymous users. The `robot` field in the `WikipediaChange` class defines whether the edit was made by a robot or not.
// MAGIC 
// MAGIC We'll use SQL here, but the same solution can be expressed with the DataFrames API.

// COMMAND ----------

enDF.registerTempTable("enDF")
val typed = sqlContext.sql("""|SELECT CASE
                              |WHEN isRobot = true THEN 'robot'
                              |WHEN isAnonymous = true THEN 'anon'
                              |ELSE 'logged-in'
                              |END AS type FROM enDF""".stripMargin)
val groupedByType = typed.groupBy("type").count()
display(groupedByType)


// COMMAND ----------

// MAGIC %md
// MAGIC ## Anonymous Edits
// MAGIC 
// MAGIC Let's do some analysis of just the anonymous editors. To make our job a little easier, we'll create another DataFrame containing _just_ the anonymous users.

// COMMAND ----------

val anonDF = enDF.filter($"isAnonymous" === true)

// COMMAND ----------

anonDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Visualizing Anonymous Editors' Locations
// MAGIC 
// MAGIC ![World Map](http://i.imgur.com/66KVZZ3.png)
// MAGIC 
// MAGIC Let's see if we can geocode the IP addresses to see where each editor is in the world. As a bonus, we'll plot the geocoded locations on a world map.
// MAGIC 
// MAGIC The server is already geocoding data for us, courtesy of the GeoLite2 data created by MaxMind (available from [www.maxmind.com](http://www.maxmind.com)). Let's take a look:

// COMMAND ----------

display(anonDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Visualizing the same data on a world map gives us a better sense of the data. Fortunately, the Databricks notebooks support a World Map graph. This graph type requires the 3-letter country codes and the counts, so that's all we're going to extract. 
// MAGIC 
// MAGIC **Note**: It's possible that some anonymous edits are associated with IP addresses that cannot be geocoded properly. So, we'll just remove any rows with a null 3-letter country code column.

// COMMAND ----------

val anonDFCounts = anonDF.select($"user".as("ipAddress"), $"geoCountryCode3".as("country"))
                         .filter(! isnull($"country"))
                         .groupBy($"country")
                         .count()
                         .select($"country", $"count")

// COMMAND ----------

// MAGIC %md If you want to assure yourself that our DataFrame is, in fact, growing in size, run this cell:

// COMMAND ----------

anonDFCounts.count()

// COMMAND ----------

display(anonDFCounts.orderBy($"count".desc))

// COMMAND ----------

// MAGIC %md Now, we can plot the counts against the world map. Just keep hitting Ctrl-Enter to update the map. Remember: The data only updates every 15 seconds or so. 
// MAGIC 
// MAGIC For future reference, here's how to configure the plot. Select the World Map, then click on the Plot Options button, and configure the resulting popup as shown here. It's important to select COUNT as the aggregation method.
// MAGIC 
// MAGIC ![World Map Plot Options](http://i.imgur.com/wf6BqhZ.png)

// COMMAND ----------

display(anonDFCounts)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Visualizing with Other JavaScript Libraries

// COMMAND ----------

// MAGIC %md
// MAGIC We can also use third-party visualization libraries, such as [Google Charts](https://developers.google.com/chart/), since Databricks allows you to specify arbitrary HTML in a notebook.
// MAGIC 
// MAGIC Let's use a Dataset to convert our data to a tuple and dump it into some HTML. Since the Google Charts geographical map supports full country names, let's create another `anonDFCounts` DataFrame with the full country name, instead of the ISO 3-letter country code.

// COMMAND ----------

val anonDFCounts2 = anonDF.select($"user".as("ipAddress"), $"geoCountry".as("country"))
                           .filter(! isnull($"country"))
                           .groupBy($"country")
                           .count()
                           .select($"country", $"count")
val data = anonDFCounts2.select($"country", $"count".cast("int")).as[(String, Int)].collect().toMap
val html = s"""
<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['geochart']});
      google.charts.setOnLoadCallback(drawRegionsMap);

      function drawRegionsMap() {

        var data = google.visualization.arrayToDataTable([
          ['Country', 'Total Edits'],
          ${data.map { case (country, count) => s"['$country', $count]" }.mkString(",\n")}
        ]);

        var options = {};

        var chart = new google.visualization.GeoChart(document.getElementById('regions_div'));

        chart.draw(data, options);
      }
    </script>
  </head>
  <body>
    <div id="regions_div" style="width: 900px; height: 500px;"></div>
  </body>
</html>"""
displayHTML(html)

// COMMAND ----------

// MAGIC %md
// MAGIC Here's another example, using the [Highmaps](http://www.highcharts.com/maps/demo/data-class-ranges) packages
// MAGIC from the popular [Highcharts](http://www.highcharts.com/) libraries.

// COMMAND ----------

val data2 = anonDFCounts.select($"country", $"count".cast("int")).as[(String, Int)].collect().toMap
val hcHTML = s"""
<html>
  <head>
    <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jquery/2.2.3/jquery.min.js"></script>
    <script src="https://code.highcharts.com/maps/highmaps.js"></script>
    <script src="https://code.highcharts.com/maps/modules/data.js"></script>
    <script src="https://code.highcharts.com/maps/modules/exporting.js"></script>
    <script src="https://code.highcharts.com/mapdata/custom/world.js"></script>

    <script type="text/javascript">
$$(document).ready(function () {

  // Prepare random data
  var data = [
    ${data2.map { case (country, count) => s"{'code': '$country', 'value': $count}" }.mkString(",\n")}
  ];


  // Initiate the chart
  $$('#container').highcharts('Map', {
    chart : {
      borderWidth : 1
    },

    colors: ['rgba(19,64,117,0.05)', 'rgba(19,64,117,0.2)', 'rgba(19,64,117,0.4)',
             'rgba(19,64,117,0.5)', 'rgba(19,64,117,0.6)', 'rgba(19,64,117,0.8)', 'rgba(19,64,117,1)'],

    title : {
      text : 'en.wikipedia.org Edits by Country (Highcharts)'
    },
    
    mapNavigation: {
      enabled: false
    },

    legend: {
      title: {
        text: 'Edits',
        style: {
          color: 'black'
        }
      },
      align: 'left',
      verticalAlign: 'bottom',
      floating: true,
      layout: 'vertical',
      valueDecimals: 0,
      backgroundColor: 'rgba(255, 255, 255, 0.85)',
      symbolRadius: 0,
      symbolHeight: 14
    },
    colorAxis: {
      dataClasses: [
      {
        to: 3
      }, 
      {
        from: 3,
        to: 10
      },
      {
        from: 10,
        to: 30
      },
      {
        from: 30,
        to: 100
      },
      {
        from: 100,
        to: 300
      },
      {
        from: 300,
        to: 1000
      },
      {
        from: 1000
      }]
    },
    series : [{
      data : data,
      mapData: Highcharts.maps['custom/world'],
      joinBy: ['iso-a3', 'code'],
      animation: true,
      name: 'Total Edits',
      states: {
        hover: {
          color: '#BADA55'
        }
      }
    }]
  });
});

    </script>
  </head>

  <body>
    <div id="container" style="height: 500px; width: 1000px">
    </div>
  </body>

</html>
"""
displayHTML(hcHTML)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Your turn
// MAGIC 
// MAGIC Time for some exercises.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Exercise 1
// MAGIC 
// MAGIC Remove the United States, Canada, Australia, New Zealand and the UK from the data and re-plot the results, to see who's editing English Wikipedia entries from countries where English is not the primary language.
// MAGIC 
// MAGIC **HINTS**: 
// MAGIC 
// MAGIC 1. This page might help: <https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3>
// MAGIC 2. Use the `display()` function and plot the results in a world map. You'll need to arrange the Plot Options appropriately. Examine the Plot Options for the previous Databricks map to figure out what to do.

// COMMAND ----------

display( <<<FILL IN>>> )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Exercise 2
// MAGIC 
// MAGIC Choose another language, and figure out where _that_ Wikipedia is being edited. The server currently serves up changes for the following Wikipedias
// MAGIC 
// MAGIC | code | language  |
// MAGIC | ---- | --------- |
// MAGIC |  da  | Danish    |
// MAGIC |  de  | German    |
// MAGIC |  en  | English   |
// MAGIC |  eo  | Esperanto |
// MAGIC |  es  | Spanish   |
// MAGIC |  fr  | French    |
// MAGIC |  it  | Italian   |
// MAGIC |  ja  | Japanese  |
// MAGIC |  nl  | Dutch     |
// MAGIC |  nn  | Norwegian |
// MAGIC |  pl  | Polish    |
// MAGIC |  ro  | Romanian  |
// MAGIC |  sv  | Swedish   |

// COMMAND ----------

// MAGIC %md ## BE SURE TO STOP YOUR STREAM!

// COMMAND ----------

stop()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Final Cleanup
// MAGIC 
// MAGIC It's not a bad idea to drop the table when you're done, as well.

// COMMAND ----------

sqlContext.sql(s"DROP TABLE $TableName")

// COMMAND ----------

