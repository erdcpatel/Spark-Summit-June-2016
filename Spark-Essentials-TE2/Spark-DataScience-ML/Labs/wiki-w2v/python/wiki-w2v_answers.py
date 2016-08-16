# Databricks notebook source exported at Tue, 27 Oct 2015 05:59:28 UTC

# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Wikipedia: Word2Vec
# MAGIC  
# MAGIC In this lab, we'll use `Word2Vec` to create vectors the words found in the Wikipedia dataset.  We'll use `Word2Vec` by passing in a `DataFrame` containing sentences.  We can pass into `Word2Vec` what length of vector to create, with larger vectors taking more time to build.
# MAGIC  
# MAGIC Be able to convert words into vectors provides us with features that can be used in traditional machine learning algorithms.  These vectors can be used to compare word similarity, sentence similarity, or even larger sections of text.

# COMMAND ----------

# MAGIC %md
# MAGIC Load the data.

# COMMAND ----------

baseDir = '/mnt/ml-class/'
dfSmall = sqlContext.read.parquet(baseDir + 'smallwiki.parquet')

# COMMAND ----------

dfSmall.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter out unwanted data.

# COMMAND ----------

import pyspark.sql.functions as func
from pyspark.sql.functions import col
filtered = dfSmall.filter((col('title') != '<PARSE ERROR>') &
                           col('redirect_title').isNull() &
                           col('text').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC Change all text to lower case.

# COMMAND ----------

lowered = filtered.select('*', func.lower(col('text')).alias('lowerText'))

# COMMAND ----------

parsed = (lowered
          .drop('text')
          .withColumnRenamed('lowerText', 'text'))

# COMMAND ----------

# MAGIC %md
# MAGIC Split the Wikipedia text into sentences.

# COMMAND ----------

pattern = r'(\. |\n{2,})'
import re
matches = re.findall(pattern, 'Wiki page. *More information*\n\n And a line\n that continues.')
print matches

# COMMAND ----------

from pyspark.ml.feature import RegexTokenizer

tokenizer = RegexTokenizer(inputCol='text', outputCol='sentences', pattern=pattern)
sentences = tokenizer.transform(parsed).select('sentences')
display(sentences)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

sentenceRDD = (sentences
               .flatMap(lambda r: r[0])
               .map(lambda x: Row(sentence=x)))

sentenceSchema = StructType([StructField('sentence', StringType())])
sentence = sqlContext.createDataFrame(sentenceRDD, sentenceSchema)

display(sentence)

# COMMAND ----------

# MAGIC %md
# MAGIC Split the sentences into words.

# COMMAND ----------

tokenizerWord = RegexTokenizer(inputCol='sentence', outputCol='words', pattern=r'\W+')
words = tokenizerWord.transform(sentence).select('words')
display(words)

# COMMAND ----------

# MAGIC %md
# MAGIC Use our `removeWords` function that we registered in wiki-eda to clean up stop words.

# COMMAND ----------

sqlContext.sql('drop table if exists words')
words.registerTempTable('words')

# COMMAND ----------

noStopWords = sqlContext.sql('select removeWords(words) as words from words') #.cache()
display(noStopWords)

# COMMAND ----------

wordVecInput = noStopWords.filter(func.size('words') != 0)
wordVecInput.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Build the `Word2Vec` model.  This take about a minute with two workers.

# COMMAND ----------

from pyspark.ml.feature import Word2Vec
word2Vec = Word2Vec(vectorSize=150, minCount=50, inputCol='words', outputCol='result', seed=0)
model = word2Vec.fit(wordVecInput)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see the model in action.

# COMMAND ----------

model.findSynonyms('house', 10).collect()

# COMMAND ----------

synonyms = model.findSynonyms('fruit', 10).collect()

for word, similarity in synonyms:
    print("{}: {}".format(word, similarity))

# COMMAND ----------

model.findSynonyms('soccer', 10).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC How can we calculate similarity between vectors and handle creating a vector for multiple words at once?

# COMMAND ----------

from pyspark.sql import Row
tmpDF = sqlContext.createDataFrame([Row(words=['fruit']),
                                    Row(words=['flower']),
                                    Row(words=['fruit', 'flower'])])

# COMMAND ----------

vFruit = model.transform(tmpDF).map(lambda r: r.result).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a cosine similarity measure.

# COMMAND ----------

from numpy.linalg import norm

def similarity(x, y):
  return x.dot(y) / (norm(x) * norm(y))

print similarity(*vFruit[:2])
print similarity(*vFruit[1:])

# COMMAND ----------

# MAGIC %md
# MAGIC `Word2Vec` handles multiple words by averaging the vectors.

# COMMAND ----------

print vFruit[0][:6]
print vFruit[1][:6]
print (vFruit[0][:6] + vFruit[1][:6]) / 2  # Averaging the word vectors gives us the vector for both words in a sentence
print vFruit[2][:6]

# COMMAND ----------

from pyspark.sql import Row
tmpDF = sqlContext.createDataFrame([Row(words=['king']),
                                    Row(words=['man']),
                                    Row(words=['woman']),
                                    Row(words=['queen'])])

v1 = model.transform(tmpDF).rdd.map(lambda r: r.result).collect()

# COMMAND ----------

k, m, w, q = v1
print similarity(k, q)
print similarity(k, (q + m)/2)
print similarity(k, m)
print similarity(q, m)
print similarity(q, k - m + w)

# COMMAND ----------
