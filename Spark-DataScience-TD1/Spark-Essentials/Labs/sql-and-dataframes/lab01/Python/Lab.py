# Databricks notebook source exported at Wed, 18 Nov 2015 00:55:25 UTC
# MAGIC %md # DataFrames Python Lab
# MAGIC 
# MAGIC For this lab, refer to the <a href="http://spark.apache.org/docs/1.6.1/api/python/pyspark.sql.html#pyspark.sql.DataFrame" target="_blank">DataFrames API documentation</a>.

# COMMAND ----------

# MAGIC %md **Remember**: In the notebook, you already have a `SQLContext` object, called `sqlContext`. However, if you need to create one yourself (e.g., for a non-notebook application), do it like this:
# MAGIC ```
# MAGIC # assuming "sc" is some existing SparkContext object
# MAGIC sqlContext = SQLContext(sc)
# MAGIC ```
# MAGIC 
# MAGIC **WARNING: Don't ever do this IN the notebook!**

# COMMAND ----------

# MAGIC %md We've already created a Parquet table containing popular first names by gender and year, for all years between 1880 and 2014. (This data comes from the United States Social Security Administration.) We can create a DataFrame from that data, by calling `sqlContext.read.parquet()`.
# MAGIC 
# MAGIC **NOTE**: That's the Spark 1.4 API. The API is slightly different in Spark 1.3.

# COMMAND ----------

# Spark 1.4 and later
df = sqlContext.read.parquet("dbfs:/mnt/training/ssn/names.parquet")

# Spark 1.3 and earlier
#df = sqlContext.parquetFile("dbfs:/mnt/training/ssn/names.parquet")

# COMMAND ----------

# MAGIC %md Let's cache our Social Security names DataFrame, to speed things up.

# COMMAND ----------

df.cache()

# COMMAND ----------

# MAGIC %md Let's take a quick look at the first 20 items of the data.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md You can also use the `display()` helper, to get more useful (and graphable) output:

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md Take a look at the data schema, as well. Note that, in this case, the schema was read from the columns (and types) in the Parquet table.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md You can create a new DataFrame that looks at a subset of the columns in the first DataFrame.

# COMMAND ----------

firstNamesDF = df.select("firstName", "year")

# COMMAND ----------

# MAGIC %md Then, you can examine the values in the `nameDF` DataFrame, using an action like `show()` or the `display()` helper: 

# COMMAND ----------

display(firstNamesDF)

# COMMAND ----------

# MAGIC %md You can also count the number of items in the data set...

# COMMAND ----------

firstNamesDF.count()

# COMMAND ----------

# MAGIC %md ...or determine how many distinct names there are.

# COMMAND ----------

firstNamesDF.select("firstName").distinct().count()

# COMMAND ----------

# MAGIC %md Let's do something a little more complicated. Let's use the original data frame to find the five most popular names for girls born in 1980. Note the `desc()` in the `orderBy()` call. `orderBy()` (which can also be invoked as `sort()`) sorts in ascending order. `desc()` causes the sort to be in _descending_ order for the column to which `desc()` is attached.

# COMMAND ----------

display(df.filter(df['year'] == 1980).
           filter(df['gender'] == 'F').
           orderBy(df['total'].desc(), df['firstName']).
           select("firstName").
           limit(5))

# COMMAND ----------

# MAGIC %md Note that we can do the same thing using the lower-level RDD operations. However, use the DataFrame operations, when possible. In general, they're more convenient. More important, though, they allow Spark to build a query plan that can be optimized through [Catalyst](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html).

# COMMAND ----------

# MAGIC %md ## Joins

# COMMAND ----------

# MAGIC %md Let's use two views of our SSN data to answer this question: How popular were the top 10 female names of 2010 back in 1930?
# MAGIC 
# MAGIC Before we can do that, though, we need to define a utility function. Spark SQL doesn't support the SQL `LOWER` function. To ensure that our data matches up properly, it'd be nice to force the names to lower case before doing the match. Fortunately, it's easy to define our own `LOWER` function:

# COMMAND ----------

lower = udf(lambda s: s.lower())

# COMMAND ----------

# MAGIC %md Okay, now we can go to work.

# COMMAND ----------

# Create a new DataFrame from the SSNA DataFrame, so that:
# - We have a lower case version of the name, for joining
# - We've weeded out the year
#
# NOTE: The aliases are necessary; otherwise, the query analyzer
# generates false equivalences between the columns.
ssn2010 = df.filter(df['year'] == 2010).\
             select(df['total'].alias("total2010"), 
                    df['gender'].alias("gender2010"), 
                    df['firstName'].alias("firstName2010"),
                    lower(df['firstName']).alias("name2010"))

# Let's do the same for 1930.
ssn1930 = df.filter(df['year'] == 1930).\
             select(df['total'].alias("total1930"), 
                    df['gender'].alias("gender1930"), 
                    df['firstName'].alias("firstName1930"),
                    lower(df['firstName']).alias("name1930"))

# Now, let's find out how popular the top 10 New York 2010 girls' names were in 1880.
# This join works fine in Scala, but not (yet) in Python.
#
# j1 = ssn2010.join(ssn1930, (ssn2010.name2010 == ssn1930.name1930) and (ssn2010.gender2010 == ssn1930.gender1930))

# So, we'll do a slightly different version.
joined = ssn2010.filter(ssn2010.gender2010 == "F").\
                 join(ssn1930.filter(ssn1930.gender1930 == "F"), ssn2010.name2010 == ssn1930.name1930).\
                 orderBy(ssn2010.total2010.desc()).\
                 limit(10).\
                 select(ssn2010.firstName2010.alias("name"), ssn1930.total1930, ssn2010.total2010)

display(joined)

# COMMAND ----------

# MAGIC %md ## Assignment

# COMMAND ----------

# MAGIC %md In the cell below, you'll see an empty function, `top_female_names_for_year`. It takes three arguments:
# MAGIC 
# MAGIC * A year
# MAGIC * A number, _n_
# MAGIC * A starting DataFrame (which will be `df`, from above)
# MAGIC 
# MAGIC It returns a new DataFrame that can be used to retrieve the top _n_ female names for that year (i.e., the _n_ names with the highest _total_ values). If there are multiple names with the same total, order those names alphabetically.
# MAGIC 
# MAGIC Write that function. To test it, run the cell _following_ the function—i.e., the one containing the `run_tests()` function. (This might take a few minutes.)

# COMMAND ----------

def top_female_names_for_year(year, n, df):
  return df.limit(10) # THIS IS NOT CORRECT! FIX IT.

# COMMAND ----------

# Transparent Tests
from test_helper import Test
def test_year(year, df):
    return [row.firstName for row in top_female_names_for_year(year, 5, df).collect()]

# COMMAND ----------

def run_tests():
  Test.assertEquals(test_year(1945, df), [u'Mary', u'Linda', u'Barbara', u'Patricia', u'Carol'], 'incorrect top 5 names for 1945')
  Test.assertEquals(test_year(1970, df), [u'Jennifer', u'Lisa', u'Kimberly', u'Michelle', u'Amy'], 'incorrect top 5 names for 1970')
  Test.assertEquals(test_year(1987, df), [u'Jessica', u'Ashley', u'Amanda', u'Jennifer', u'Sarah'], 'incorrect top 5 names for 1987')
  Test.assertTrue(len(test_year(1945, df)) <= 5, 'list not limited to 5 names')
  Test.assertTrue(u'James' not in test_year(1945, df), 'male names not filtered')
  Test.assertTrue(test_year(1945, df) != [u'Linda', u'Linda', u'Linda', u'Linda', u'Mary'], 'year not filtered')
  Test.assertEqualsHashed(test_year(1880, df), "2038e2c0bb0b741797a47837c0f94dbf24123447", "incorrect top 5 names for 1880")
  
run_tests()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Solution
# MAGIC 
# MAGIC If you're stuck, and you're really not sure how to proceed, feel free to check out the solution. You'll find it in the same folder as the lab.

# COMMAND ----------

