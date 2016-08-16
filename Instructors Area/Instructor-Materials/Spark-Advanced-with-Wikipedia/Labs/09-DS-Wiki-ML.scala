// Databricks notebook source exported at Wed, 1 Jun 2016 21:13:02 UTC
// MAGIC %md Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

// COMMAND ----------

// MAGIC %md ## Machine Learning Pipeline: TF-IDF and K-Means

// COMMAND ----------

val wikiDF = sqlContext.read.parquet("/mnt/wikipedia-readonly/en_wikipedia/march_5_snapshot/flattenedParquet_updated3days_30partitions/").cache()

// COMMAND ----------

wikiDF.columns

// COMMAND ----------

wikiDF.show(5)

// COMMAND ----------

// MAGIC %md Lower case the text:

// COMMAND ----------

import org.apache.spark.sql.functions._

val wikiLoweredDF = wikiDF.select($"*", lower($"text").as("lowerText"))

// COMMAND ----------

wikiLoweredDF.show(2)

// COMMAND ----------

// MAGIC %md #### Set up the ML Pipeline:

// COMMAND ----------

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, HashingTF, IDF, Normalizer}

// COMMAND ----------

// MAGIC %md ##### Step 1: Natural Language Processing: RegexTokenizer: Convert the lowerText col to a bag of words

// COMMAND ----------

val tokenizer = new RegexTokenizer()
  .setInputCol("lowerText")
  .setOutputCol("words")
  .setPattern("\\W+")

// COMMAND ----------

val wikiWordsDF = tokenizer.transform(wikiLoweredDF)

// COMMAND ----------

wikiWordsDF.printSchema

// COMMAND ----------

wikiWordsDF.select("words").first

// COMMAND ----------

// MAGIC %md ##### Step 2: Natural Language Processing: StopWordsRemover: Remove Stop words

// COMMAND ----------

val remover = new StopWordsRemover()
  .setInputCol("words")
  .setOutputCol("noStopWords")

// COMMAND ----------

val noStopWordsListDF = remover.transform(wikiWordsDF)

// COMMAND ----------

noStopWordsListDF.printSchema

// COMMAND ----------

noStopWordsListDF.select("id", "title", "words", "noStopWords").show(15)

// COMMAND ----------

// MAGIC %md ##### Step 3: HashingTF

// COMMAND ----------

// More features = more complexity and computational time and accuracy

val hashingTF = new HashingTF().setInputCol("noStopWords").setOutputCol("hashingTF").setNumFeatures(20000)
val featurizedDataDF = hashingTF.transform(noStopWordsListDF)

// COMMAND ----------

featurizedDataDF.printSchema

// COMMAND ----------

featurizedDataDF.select("id", "title", "noStopWords", "hashingTF").show(7)

// COMMAND ----------

// MAGIC %md ##### Step 4: IDF

// COMMAND ----------

// This will take 3 - 4 mins to run
val idf = new IDF().setInputCol("hashingTF").setOutputCol("idf")
val idfModel = idf.fit(featurizedDataDF)

// COMMAND ----------

// MAGIC %md ##### Step 5: Normalizer

// COMMAND ----------

// A normalizer is a common operation for text classification.

// It simply gets all of the data on the same scale... for example, if one article is much longer and another, it'll normalize the scales for the different features.

// If we don't normalize, an article with more words would be weighted differently

val normalizer = new Normalizer()
  .setInputCol("idf")
  .setOutputCol("features")

// COMMAND ----------

// MAGIC %md ##### Step 6: k-means & tie it all together...

// COMMAND ----------

// This will take over 1 hour to run!

/*
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
 
val kmeans = new KMeans()
  .setFeaturesCol("features")
  .setPredictionCol("prediction")
  .setK(100)
  .setSeed(0) // for reproducability
 
val pipeline = new Pipeline()
  .setStages(Array(tokenizer, remover, hashingTF, idf, normalizer, kmeans))  
 
val model = pipeline.fit(wikiLoweredDF)
*/

// COMMAND ----------

// MAGIC %md Load the model built from the 5 million articles instead:
// MAGIC 
// MAGIC This will take a little over 3 minutes

// COMMAND ----------

val model2 = org.apache.spark.ml.PipelineModel.load("/mnt/wikipedia-readonly/models/5mill_articles")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at a sample of the data to see if we can see a pattern between predicted clusters and titles.

// COMMAND ----------

val predictionsDF = model2.transform(wikiLoweredDF)

// COMMAND ----------

predictionsDF.columns

// COMMAND ----------

predictionsDF.show(10)

// COMMAND ----------

predictionsDF.select($"title", $"prediction").show(10)

// COMMAND ----------

predictionsDF.groupBy("prediction").count().show(100)

// COMMAND ----------

//Indonesia and Asia?
display(predictionsDF.filter("prediction = 31").select("title", "prediction").limit(30))

// COMMAND ----------

//Norway?
display(predictionsDF.filter("prediction = 32").select("title", "prediction").limit(30))

// COMMAND ----------

//Baseball??
display(predictionsDF.filter("prediction = 45").select("title", "prediction").limit(30))

// COMMAND ----------

//Music and film?
display(predictionsDF.filter("prediction = 82").select("title", "prediction").limit(30))

// COMMAND ----------

// Singers and Actors?
display(predictionsDF.filter("prediction = 83").select("title", "prediction").limit(20))

// COMMAND ----------

//Politics?
display(predictionsDF.filter("prediction = 37").select("title", "prediction").limit(30))

// COMMAND ----------

predictionsDF.filter($"title" === "Apache Spark").withColumn("num_words", size($"words")).select("title", "num_words", "prediction").show(10)

// COMMAND ----------

// What will the cluster contain? big data? technology? software?
display(predictionsDF.filter("prediction = 5").withColumn("num_words", size($"words")).select("title", "num_words", "prediction").limit(25))
