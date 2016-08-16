# Databricks notebook source exported at Mon, 14 Dec 2015 03:03:49 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Wikipedia: LDA
# MAGIC  
# MAGIC This lab explores building a Latent Dirichlet allocation (LDA) model.  We'll use LDA to generate 10 topics that correspond to the Wikipedia data.  These topics will correspond to words found in the articles.  We'll take an article and see which of the topics it is associated with.  LDA won't just categories the article into one category but will give a numeric value that corresponds to the articles relevance to each of the 10 topics.
# MAGIC  
# MAGIC LDA is currently only implemented in MLlib.  Details can be found in the [MLlib clustering guide](http://spark.apache.org/docs/latest/mllib-clustering.html#latent-dirichlet-allocation-lda).
# MAGIC  
# MAGIC Additional details about the algorithm can be found on [Wikipedia](http://spark.apache.org/docs/latest/mllib-clustering.html#latent-dirichlet-allocation-lda).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load in the data.
# MAGIC  
# MAGIC First, we'll load the `DataFrame` which was called `noStopWords` in the wiki-etl-eda notebook.  This way we can avoid some of the pre-processing steps.

# COMMAND ----------

# MAGIC %md
# MAGIC Recall that even though we removed a series of stop words, the word counts for certain works were very high and they were words that didn't seem to convey much meaning for an article.  We'll view words ordered by their frequency to find a cutoff point for removing additional stop words.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a new list of stop words based on our cutoff.

# COMMAND ----------

# MAGIC %md
# MAGIC Use `StopWordsRemover` to remove our new list of stop words from the dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC Next, review a few examples to see if we have improved our word list.

# COMMAND ----------

# MAGIC %md
# MAGIC Then, we'll use `CountVectorizer` to obtain word counts per article.  Note that these will be stored as `SparseVectors` within our `DataFrame`.

# COMMAND ----------

# MAGIC %md
# MAGIC We'll save our vocabulary so that we can skip some steps if we want to use the LDA model later.

# COMMAND ----------

# MAGIC %md
# MAGIC This is how we can reload in the vocabulary from a text file.

# COMMAND ----------

# MAGIC %md
# MAGIC Recall that `CountVectorizer` returns a `SparseVector`.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's go ahead and build that LDA model.  Since `LDA` falls within `mllib` it doesn't take in a `DataFrame`.  We need to provide an `RDD`.  `LDA` expects an `RDD` that contains a tuple of (index, `Vector`) pairs.  Under the hood LDA uses GraphX which performs shuffles which change the order of the data, so the usual `mllib` strategy of zipping the results together will not work.  With LDA we'll use joins based on the articles indices.

# COMMAND ----------

# MAGIC %md
# MAGIC The below command was used to save the LDA model for later use.

# COMMAND ----------

# MAGIC %md
# MAGIC This is how we load back in the saved model.

# COMMAND ----------

# MAGIC %md
# MAGIC What's stored in an LDA model?

# COMMAND ----------

# MAGIC %md
# MAGIC Let's view the first three words that are most relevant for our 10 topics.  The first array references work indices and is their relevance to this topic.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use our `CountVectorizer` vocabulary to generate more readable topics.  Note that this makes use of LDA's `describeTopics`.

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's view the top documents for our 10 topics.  Note that this makes use of `topDocumentsPerTopic` but joins in titles to make the results more readable.  `topDocumentsPerTopic` is available for `DistributedLDAModels`.

# COMMAND ----------

# MAGIC %md
# MAGIC How many articles are we working with?

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll search for an article based on a keyword and then see how that article is classified into topics.  The below example searches for european football related articles.

# COMMAND ----------

# MAGIC %md
# MAGIC The article about Belgium seems interesting.  Let's use id 2880 and see what topics are ranked as most relevant.  Note that this uses `topicDistributions` which is a `DistributedLDAModel` method.

# COMMAND ----------

# MAGIC %md
# MAGIC We'll register our `DataFrame` with ids and titles, so that we can query it from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC select title, id from idTitle where title like "%$Query%"

# COMMAND ----------
