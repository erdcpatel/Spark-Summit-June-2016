// Databricks notebook source exported at Wed, 1 Jun 2016 22:48:03 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC # Explore English Wikipedia via DataFrames and RDD API
// MAGIC ### Time to complete: 20 minutes
// MAGIC 
// MAGIC #### Business Questions:
// MAGIC 
// MAGIC * Question # 1) What percentage of Wikipedia articles were edited in the past month (before the data was collected)?
// MAGIC * Question # 2) How many of the 1 million articles were last edited by ClueBot NG, an anti-vandalism bot?
// MAGIC * Question # 3) Which user in the 1 million articles was the last editor of the most articles?
// MAGIC * Question # 4) Can you display the titles of the articles in Wikipedia that contain a particular word?
// MAGIC * Question # 5) Can you extract out all of the words from the Wikipedia articles? (bag of words)
// MAGIC * Question # 6) What are the top 15 most common words in the English language?
// MAGIC * Question # 7) After removing stop words, what are the top 10 most common words in the english language? 
// MAGIC * Question # 8) How many distinct/unique words are in noStopWordsListDF?
// MAGIC 
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Work with 2% of the sum of all human knowledge!

// COMMAND ----------

// MAGIC %md Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting to know the Data
// MAGIC Locate the Parquet data from the demo using `dbutils`:

// COMMAND ----------

display(dbutils.fs.ls("/mnt/wikipedia-readonly/en_wikipedia/march_5_snapshot/flattenedParquet_updated3days_30partitions/"))

// COMMAND ----------

// MAGIC %md These are the 30 parquet files (~660 MB total) from the English Wikipedia Articles (March 5, 2016 snapshot) that were last updated between March 3, 2016 - March 5, 2016, inclusive.

// COMMAND ----------

// MAGIC %md Load the articles into memory and lazily cache them:

// COMMAND ----------

val wikiDF = sqlContext.read.parquet("dbfs:/mnt/wikipedia-readonly/en_wikipedia/march_5_snapshot/flattenedParquet_updated3days_30partitions/").cache

// COMMAND ----------

// MAGIC %md Notice how fast `printSchema()` runs... this is because we can derive the schema from the Parquet metadata:

// COMMAND ----------

wikiDF.printSchema()

// COMMAND ----------

// MAGIC %md Look at the first 5 rows:

// COMMAND ----------

display(wikiDF.limit(5))

// COMMAND ----------

// MAGIC %md Let's count how many total articles we have. (Note that when using a local mode cluster, the next command will take **1 - 2 minutes**, so you may want to skip ahead and read some of the next cells:

// COMMAND ----------

// You can monitor the progress of this count + cache materialization via the Spark UI's storage tab
printf("%,d articles\n", wikiDF.count)

// COMMAND ----------

// MAGIC %md The DataFrame will use 1.9 GB of Memory (of the 2.4 GB total available).

// COMMAND ----------

// MAGIC %md This lab is meant to introduce you to working with unstructured text data in the Wikipedia articles. 

// COMMAND ----------

// MAGIC %md In this lab, among other tasks, we will apply basic Natural Language Processing to the article text to extract out a bag of words.

// COMMAND ----------

// MAGIC %md By now the `.count()` operation might be completed. Go back up and check and only proceed after the count has completed. You should see the count's results as 111,495 items.

// COMMAND ----------

// MAGIC %md Run `.count()` again to see the speed increase:

// COMMAND ----------

printf("%,d articles\n", wikiDF.count)

// COMMAND ----------

// MAGIC %md That's pretty impressive! We can scan through 111 thousand recent articles of English Wikipedia using a single 3.7 GB Executor in under 1 second.

// COMMAND ----------

// MAGIC %md Register the DataFrame as a temporary table, so we can execute SQL against it:

// COMMAND ----------

wikiDF.registerTempTable("wikipedia")

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #1:
// MAGIC ** How many days of data is in the DataFrame? **

// COMMAND ----------

// MAGIC %md ** Challenge 1:**  Can you write this query using DataFrames? Hint: Start by importing the sql.functions.

// COMMAND ----------

// Type your answer here...


// COMMAND ----------

// MAGIC %md At the time of collection (March 5, 2016) Wikipedia had 5.096 million articles. What % of Wikipedia are we working with?

// COMMAND ----------

printf("%.2f%%\n", wikiDF.count()/5096292.0*100)

// COMMAND ----------

// MAGIC %md About 2% of English Wikipedia. Here are 10 such articles:

// COMMAND ----------

// MAGIC %sql SELECT title, lastrev_pdt_time FROM wikipedia LIMIT 10;

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #2:
// MAGIC ** How many of the 111 thousand articles were last edited by <a href="https://en.wikipedia.org/wiki/User:ClueBot_NG" target="_blank">ClueBot NG</a>, an anti-vandalism bot? **

// COMMAND ----------

// MAGIC %md **Challenge 2:**  Write a SQL query to answer this question. Then write a second query to display the first 10.
// MAGIC Hint: The username to search for is "ClueBot NG".


// COMMAND ----------

// Type your answer here... (tip: you must remove this comment)


// COMMAND ----------

// MAGIC %md **Challenge 3:** Update your previous query to show the first 10 records

// COMMAND ----------

// Type your answer here... (tip: you most remove this comment)


// COMMAND ----------

// MAGIC %md You can study the specifc revisions like so: **https://<span></span>en.wikipedia.org/?diff=####**
// MAGIC 
// MAGIC For example: <a href="https://en.wikipedia.org/?diff=708113872" target="_blank">https://<span></span>en.wikipedia.org/?diff=708113872</a>

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #3:
// MAGIC ** Which user in the 111 thousand articles was the last editor of the most articles? **

// COMMAND ----------

// MAGIC %md Here's a slightly more complicated query:

// COMMAND ----------

// MAGIC %sql SELECT contributorusername, COUNT(contributorusername) FROM wikipedia GROUP BY contributorusername ORDER BY COUNT(contributorusername) DESC; 

// COMMAND ----------

// MAGIC %md Hmm, looks are bots are quite active in maintaining Wikipedia.

// COMMAND ----------

// MAGIC %md Interested in learning more about the bots that edit Wikipedia? Check out: <a href="https://en.wikipedia.org/wiki/Wikipedia:List_of_bots_by_number_of_edits" target="_blank">Wikipedia:List_of_bots_by_number_of_edits</a>

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #4:
// MAGIC ** Can you display the titles of the articles in Wikipedia that contain a particular word? **

// COMMAND ----------

// MAGIC %md Start by registering a User Defined Function (UDF) that can search for a string in the text of an article.

// COMMAND ----------

// Register a function that can search if a string is found.

val containsWord = (s: String, w: String) => {
  (s != null && s.indexOfSlice(w) >= 0)
}
sqlContext.udf.register("containsWord", containsWord)

// COMMAND ----------

// MAGIC %md Verify that the `containsWord` function is working as intended:

// COMMAND ----------

// Look for the word 'test' in the first string
containsWord("hello astronaut, how's space?", "test")

// COMMAND ----------

// Look for the word 'space' in the first string
containsWord("hello astronaut, how's space?", "space")

// COMMAND ----------

// MAGIC %md  Use a parameterized query so you can easily change the word to search for:

// COMMAND ----------

// MAGIC %sql  select title from wikipedia where containsWord(text, '$word')

// COMMAND ----------

// MAGIC %md Try typing in `NASA` or `Manhattan` into the search box above and hit SHIFT + ENTER.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #5:
// MAGIC ** Can you extract out all of the words from the Wikipedia articles? ** (Create a bag of words)

// COMMAND ----------

// MAGIC %md Use Spark.ml's RegexTokenizer to read an input column of 'text' and write a new output column of 'words':

// COMMAND ----------

import org.apache.spark.ml.feature.RegexTokenizer
 
val tokenizer = new RegexTokenizer()
  .setInputCol("text")
  .setOutputCol("words")
  .setPattern("\\W+")

val wikiWordsDF = tokenizer.transform(wikiDF)

// COMMAND ----------

wikiWordsDF.show(5)

// COMMAND ----------

wikiWordsDF.select($"title", $"words").first

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #6:
// MAGIC ** What are the top 15 most common words in the English language? ** Compute this only on a random 10% of the 111 thousand articles.

// COMMAND ----------

// MAGIC %md For this analysis, we should get reasonably accurate results even if we work on just 10% of the 111 thousand articles. Plus, this will speed things up tremendously. Note that this means about 11,000 articles

// COMMAND ----------

val tenPercentDF = wikiWordsDF.sample(false, .10, 555)

// COMMAND ----------

printf("%,d words (total)\n", wikiWordsDF.count)
printf("%,d words (sample)\n", tenPercentDF.count)

// COMMAND ----------

// MAGIC %md Note that the `words` column contains arrays of Strings:

// COMMAND ----------

tenPercentDF.select($"words")

// COMMAND ----------

// MAGIC %md Let's explode the `words` column into a table of one word per row:

// COMMAND ----------

import org.apache.spark.sql.{functions => func}
val tenPercentWordsListDF = tenPercentDF.select(func.explode($"words").as("word"))

// COMMAND ----------

display(tenPercentWordsListDF)

// COMMAND ----------

printf("%,d words\n", tenPercentWordsListDF.cache().count())

// COMMAND ----------

// MAGIC %md The onePercentWordsListDF contains 29.8 million words.

// COMMAND ----------

// MAGIC %md Finally, run a wordcount on the exploded table:

// COMMAND ----------

val wordGroupCountDF = tenPercentWordsListDF
                      .groupBy("word")  // group
                      .agg(func.count("word").as("counts"))  // aggregate
                      .sort(func.desc("counts"))  // sort

wordGroupCountDF.show(15)

// COMMAND ----------

// MAGIC %md These would be good <a href="https://en.wikipedia.org/wiki/Stop_words" target="_blank">stop words</a> to filter out before running Natural Language Processing algorithms on our data.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #7:
// MAGIC ** After removing stop words, what are the top 10 most common words in the english language? ** Compute this only on a random 10% of the 111 thousand articles.

// COMMAND ----------

// MAGIC %md Use Spark.ml's stop words remover:

// COMMAND ----------

import org.apache.spark.ml.feature.StopWordsRemover

val remover = new StopWordsRemover()
  .setInputCol("words")
  .setOutputCol("noStopWords")

// COMMAND ----------

// MAGIC %md Notice the removal of words like "about", "the",  etc:

// COMMAND ----------

remover.transform(tenPercentDF).select("id", "title", "words", "noStopWords").show(15)

// COMMAND ----------

val noStopWordsListDF = remover.transform(tenPercentDF).select(func.explode($"noStopWords").as("word"))

// COMMAND ----------

noStopWordsListDF.show(7)

// COMMAND ----------

// MAGIC %md The tenPercentWordsListDF (which included stop words) contained 29.8 million words. How many words are in the noStopWordsListDF?

// COMMAND ----------

printf("%,d words\n", noStopWordsListDF.cache.count)

// COMMAND ----------

// MAGIC %md 22.4 million words remain. That means about 7.4 million words in our 10% sample were actually stop words.

// COMMAND ----------

// MAGIC %md Finally, let's see the top 15 words now:

// COMMAND ----------

val noStopWordsGroupCount = noStopWordsListDF
                      .groupBy("word")  // group
                      .agg(func.count("word").as("counts"))  // aggregate
                      .sort(func.desc("counts"))  // sort

noStopWordsGroupCount.show(15)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #8:
// MAGIC ** How many distinct/unique words are in noStopWordsListDF?**

// COMMAND ----------

printf("%,d distinct words", noStopWordsListDF.distinct.count)

// COMMAND ----------

// MAGIC %md Looks like the Wikipedia corpus has around 700,000 unique words. Probably a lot of these are rare scientific words, numbers, etc.

// COMMAND ----------

// MAGIC %md This concludes the English Wikipedia NLP lab.
