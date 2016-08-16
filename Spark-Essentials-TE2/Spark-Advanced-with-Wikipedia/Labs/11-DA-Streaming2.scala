// Databricks notebook source exported at Tue, 17 May 2016 21:41:45 UTC
// MAGIC %md #### Business question:
// MAGIC 
// MAGIC * Question # 1) How many Edits occur every 3 seconds to the English Wikipedia vs. another language?

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._

// COMMAND ----------

// The batch interval sets how we collect data for, before analyzing it in a batch
val BatchInterval = Seconds(3)

// We'll use a unique server for the English edit stream
val EnglishStreamingServerHost  = "52.89.53.194"
val EnglishStreamingServerPort  = 9002 //en

// We'll use a unique server for all the other language edit streams
val MiscLangStreamingServerHost  = "54.68.10.240"

val SpanishStreamingServerPort  = 9007 //es
val GermanStreamingServerPort  = 9003 //de
val FrenchStreamingServerPort  = 9004 //fr
val RussianStreamingServerPort  = 9005 //ru
val ItalianStreamingServerPort  = 9006 //it

// COMMAND ----------

// MAGIC %md Create a new `StreamingContext`, using the SparkContext and batch interval:

// COMMAND ----------

val ssc = new StreamingContext(sc, BatchInterval)

// COMMAND ----------

// MAGIC %md Create two DStreams, one for English and another for a language of your choosing:

// COMMAND ----------

val baseEnDSTREAM = ssc.socketTextStream(EnglishStreamingServerHost, EnglishStreamingServerPort)

// COMMAND ----------

val baseDeDSTREAM = ssc.socketTextStream(MiscLangStreamingServerHost, GermanStreamingServerPort)

// COMMAND ----------

// MAGIC %md For each DStream, parse the incoming JSON and register a new temporary table every batch interval:

// COMMAND ----------

baseEnDSTREAM.window(Minutes(2)).transform(_.coalesce(1)).foreachRDD { rdd =>
  if(! rdd.isEmpty) {
    sqlContext.read.json(rdd).registerTempTable("English_Edits")
  }
}

baseDeDSTREAM.window(Minutes(2)).transform(_.coalesce(1)).foreachRDD { rdd => 

  if (! rdd.isEmpty) {
    sqlContext.read.json(rdd).registerTempTable("German_Edits")
    sqlContext.table("German_Edits").unionAll(sqlContext.table("English_Edits")).registerTempTable("Both_Edits")
  }
}

// COMMAND ----------

  ssc.remember(Minutes(5))  // To make sure data is not deleted by the time we query it interactively

// COMMAND ----------

ssc.start

// COMMAND ----------

// MAGIC %md Notice the table keeps growing now:

// COMMAND ----------

// MAGIC %sql select count(*) from English_Edits

// COMMAND ----------

// MAGIC %sql select * from English_Edits

// COMMAND ----------

// MAGIC %sql select * from German_Edits

// COMMAND ----------

// MAGIC %sql SELECT first(channel) AS Language, Count(*) AS Edit_Count FROM English_Edits
// MAGIC UNION
// MAGIC SELECT first(channel) AS Language, Count(*)  AS Edit_Count FROM German_Edits;

// COMMAND ----------

// MAGIC %sql 
// MAGIC select "english" AS language, substring(timestamp, 0, 19) as timestamp, count(*) AS count from English_Edits GROUP BY timestamp ORDER BY timestamp

// COMMAND ----------

// MAGIC %sql 
// MAGIC select channel, substring(timestamp, 0, 19) as timestamp, count(*) AS count from Both_Edits GROUP BY timestamp, channel ORDER BY timestamp

// COMMAND ----------

// Optional just to stop
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------


