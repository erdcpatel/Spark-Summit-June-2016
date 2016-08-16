# Databricks notebook source exported at Tue, 22 Sep 2015 16:00:36 UTC
# MAGIC %md
# MAGIC 
# MAGIC #![Spark Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/ta_Spark-logo-small.png)
# MAGIC ## Solutions to: A Visual Guide to Spark's API
# MAGIC ### Time to complete: 30 minutes
# MAGIC #### This lab will introduce you to using Apache Spark 1.3 with the Python API. We will explore common transformations and actions including
# MAGIC * Actions: Collect, Count, GetNumPartitions, Reduce, Aggregate, Max, Min, Sum, Mean, Variance, Stdev, CountByKey, SaveAsTextFile, 
# MAGIC * Transformations + MISC operations: Map, Filter, FlatMap, GroupBy, GroupByKey, MapPartitions, MapPartitionsWithIndex, Sample, Union, Join, Distinct, Coalese, KeyBy, PartitionBy, Zip
# MAGIC 
# MAGIC 
# MAGIC Note that these images were inspired by Jeff Thomson's 67 "PySpark images".

# COMMAND ----------

# MAGIC %md ### Collect
# MAGIC 
# MAGIC Action / To Driver: Return all items in the RDD to the driver in a single list
# MAGIC 
# MAGIC Start with this action, since it is used in all of the examples.
# MAGIC 
# MAGIC ![](http://i.imgur.com/DUO6ygB.png)

# COMMAND ----------

x = sc.parallelize([1,2,3], 2)
y = x.collect()
print(x.glom().collect()) # glom() flattens elements on the same partition
print(y)

# COMMAND ----------

# MAGIC %md ## Transformations
# MAGIC 
# MAGIC Create a new RDD from one or more RDDs

# COMMAND ----------

# MAGIC %md ###Map
# MAGIC 
# MAGIC Transformation / Narrow: Return a new RDD by applying a function to each element of this RDD
# MAGIC 
# MAGIC ![](http://i.imgur.com/PxNJf0U.png)

# COMMAND ----------

x = sc.parallelize(["b", "a", "c"])
y = x.map(lambda z: (z, 1))
print(x.collect())
print(y.collect())


# COMMAND ----------

# MAGIC %md ####**Try it!** change the indicated line to produce squares of the original numbers

# COMMAND ----------

#Lab exercise:

x = sc.parallelize([1,2,3,4])
y = x.map(lambda n: n*n) #CHANGE the lambda to take a number and returns its square
print(x.collect())
print(y.collect())

# COMMAND ----------

# MAGIC %md #### Filter
# MAGIC 
# MAGIC Transformation / Narrow: Return a new RDD containing only the elements that satisfy a predicate
# MAGIC 
# MAGIC ![](http://i.imgur.com/GFyji4U.png)

# COMMAND ----------

x = sc.parallelize([1,2,3])
y = x.filter(lambda x: x%2 == 1) #keep odd values 
print(x.collect())
print(y.collect())

# COMMAND ----------

# MAGIC %md ####**Try it!** Change the sample to keep even numbers

# COMMAND ----------

#Lab exercise:
x = sc.parallelize([1,2,3])
y = x.filter(lambda n:n%2 == 0) #add a lambda parameter to keep only even numbers
print(x.collect())
print(y.collect())

# COMMAND ----------

# MAGIC %md ### FlatMap
# MAGIC 
# MAGIC Transformation / Narrow: Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results
# MAGIC 
# MAGIC ![](http://i.imgur.com/TsSUex8.png)

# COMMAND ----------

x = sc.parallelize([1,2,3])
y = x.flatMap(lambda x: (x, x*100, 42))
print(x.collect())
print(y.collect())


# COMMAND ----------

# MAGIC %md ### GroupBy
# MAGIC 
# MAGIC Transformation / Wide: Group the data in the original RDD. Create pairs where the key is the output of a user function, and the value is all items for which the function yields this key.
# MAGIC 
# MAGIC ![](http://i.imgur.com/gdj0Ey8.png)

# COMMAND ----------

x = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
y = x.groupBy(lambda w: w[0])
print [(k, list(v)) for (k, v) in y.collect()]


# COMMAND ----------

# MAGIC %md ### GroupByKey
# MAGIC 
# MAGIC Transformation / Wide: Group the values for each key in the original RDD. Create a new pair where the original key corresponds to this collected group of values.
# MAGIC 
# MAGIC ![](http://i.imgur.com/TlWRGr2.png)

# COMMAND ----------

x = sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
y = x.groupByKey()
print(x.collect())
print(list((j[0], list(j[1])) for j in y.collect()))


# COMMAND ----------

# MAGIC %md ### MapPartitions
# MAGIC 
# MAGIC Transformation / Narrow: Return a new RDD by applying a function to each partition of this RDD
# MAGIC 
# MAGIC ![](http://i.imgur.com/dw8QOLX.png)

# COMMAND ----------

x = sc.parallelize([1,2,3], 2)

def f(iterator): yield sum(iterator); yield 42

y = x.mapPartitions(f)

print(x.glom().collect())
print(y.glom().collect())


# COMMAND ----------

# MAGIC %md ### MapPartitionsWithIndex
# MAGIC 
# MAGIC Transformation / Narrow: Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition
# MAGIC 
# MAGIC ![](http://i.imgur.com/3cGvAF7.png)

# COMMAND ----------

x = sc.parallelize([1,2,3], 2)

def f(partitionIndex, iterator): yield (partitionIndex, sum(iterator))

y = x.mapPartitionsWithIndex(f)

print(x.glom().collect())
print(y.glom().collect())


# COMMAND ----------

# MAGIC %md ### Sample
# MAGIC 
# MAGIC Transformation / Narrow: Return a new RDD containing a statistical sample of the original RDD
# MAGIC 
# MAGIC ![](http://i.imgur.com/LJ56nQq.png)

# COMMAND ----------

x = sc.parallelize([1, 2, 3, 4, 5])
y = x.sample(False, 0.4, 42)
print(x.collect())
print(y.collect())


# COMMAND ----------

# MAGIC %md ### Union
# MAGIC 
# MAGIC Transformation / Narrow: Return a new RDD containing all items from two original RDDs. Duplicates are not culled.
# MAGIC 
# MAGIC ![](http://i.imgur.com/XFpbqZ8.png)

# COMMAND ----------

x = sc.parallelize([1,2,3], 2)
y = sc.parallelize([3,4], 1)
z = x.union(y)
print(z.glom().collect())


# COMMAND ----------

# MAGIC %md ### Join
# MAGIC 
# MAGIC Transformation / Wide: Return a new RDD containing all pairs of elements having the same key in the original RDDs
# MAGIC 
# MAGIC ![](http://i.imgur.com/YXL42Nl.png)

# COMMAND ----------

x = sc.parallelize([("a", 1), ("b", 2)])
y = sc.parallelize([("a", 3), ("a", 4), ("b", 5)])
z = x.join(y)
print(z.collect())


# COMMAND ----------

# MAGIC %md ####**Try it!** Join the RDDs so that each company's name and stock price are collected into a tuple value, whose key is the company ticker symbol.

# COMMAND ----------

x = sc.parallelize([("TWTR", "Twitter"), ("GOOG", "Google"), ("AAPL", "Apple")])
y = sc.parallelize([("TWTR", 36), ("GOOG", 532), ("AAPL", 127)])

print(x.join(y).collect())
#Add code here to perform a join and print the result

# COMMAND ----------

# MAGIC %md ### Distinct
# MAGIC 
# MAGIC Transformation / Wide: Return a new RDD containing distinct items from the original RDD (omitting all duplicates)
# MAGIC 
# MAGIC ![](http://i.imgur.com/Vqgy2a4.png)

# COMMAND ----------

x = sc.parallelize([1,2,3,3,4])
y = x.distinct()

print(y.collect())


# COMMAND ----------

# MAGIC %md ### Coalesce
# MAGIC 
# MAGIC Transformation / Narrow or Wide: Return a new RDD which is reduced to a smaller number of partitions
# MAGIC 
# MAGIC ![](http://i.imgur.com/woQiM7E.png)

# COMMAND ----------

x = sc.parallelize([1, 2, 3, 4, 5], 3)
y = x.coalesce(2)
print(x.glom().collect())
print(y.glom().collect())


# COMMAND ----------

# MAGIC %md ### KeyBy
# MAGIC 
# MAGIC Transformation / Narrow: Create a Pair RDD, forming one pair for each item in the original RDD. The pair’s key is calculated from the value via a user-supplied function.
# MAGIC 
# MAGIC ![](http://i.imgur.com/nqYhDW5.png)

# COMMAND ----------

x = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
y = x.keyBy(lambda w: w[0])
print y.collect()

# COMMAND ----------

# MAGIC %md ####**Try it!** Create an RDD from this list, and then use .keyBy to create a pair RDD where the state abbreviation is the key and the city + state is the value (e.g., ("NY", "New York, NY")) ... For extra credit, add a .map that strips out the redundant state abbreviation to yield pairs like ("NY", "New York").

# COMMAND ----------

data = ["New York, NY", "Philadelphia, PA", "Denver, CO", "San Francisco, CA"]
# Add code to parallelize the list to an RDD
# call .keyBy on the RDD to create an RDD of pairs
x = sc.parallelize(data)

pairs = x.keyBy(lambda s:s.split(", ")[1])
print(pairs.collect())

stateCityPairs = pairs.map(lambda t:(t[0], t[1].split(", ")[0]))
print(stateCityPairs.collect())


# COMMAND ----------

# MAGIC %md ### PartitionBy
# MAGIC 
# MAGIC Transformation / Wide: Return a new RDD with the specified number of partitions, placing original items into the partition returned by a user supplied function
# MAGIC 
# MAGIC ![](http://i.imgur.com/QHDWwYv.png)

# COMMAND ----------

x = sc.parallelize([('J','James'),('F','Fred'), ('A','Anna'),('J','John')], 3)

y = x.partitionBy(2, lambda w: 0 if w[0] < 'H' else 1)

print x.glom().collect()
print y.glom().collect()

# COMMAND ----------

# MAGIC %md ### Zip
# MAGIC 
# MAGIC Transformation / Narrow: Return a new RDD containing pairs whose key is the item in the original RDD, and whose value is that item’s corresponding element (same partition, same index) in a second RDD
# MAGIC 
# MAGIC ![](http://i.imgur.com/5J0lg6g.png)

# COMMAND ----------

x = sc.parallelize([1, 2, 3])
y = x.map(lambda n:n*n)
z = x.zip(y)

print(z.collect())

# COMMAND ----------

# MAGIC %md ## Actions
# MAGIC 
# MAGIC Calculate a result (e.g., numeric data or creata a non-RDD data structure), or produce a side effect, such as writing output to disk

# COMMAND ----------

# MAGIC %md ### GetNumPartitions
# MAGIC 
# MAGIC Action / To Driver: Return the number of partitions in RDD
# MAGIC 
# MAGIC ![](http://i.imgur.com/9yhDsVX.png)

# COMMAND ----------

x = sc.parallelize([1,2,3], 2)
y = x.getNumPartitions()

print(x.glom().collect())
print(y)

# COMMAND ----------

# MAGIC %md ### Reduce
# MAGIC 
# MAGIC Action / To Driver: Aggregate all the elements of the RDD by applying a user function pairwise to elements and partial results, and return a result to the driver
# MAGIC 
# MAGIC ![](http://i.imgur.com/R72uzwX.png)

# COMMAND ----------

x = sc.parallelize([1,2,3,4])
y = x.reduce(lambda a,b: a+b)

print(x.collect())
print(y)

# COMMAND ----------

# MAGIC %md ### Aggregate
# MAGIC 
# MAGIC Action / To Driver: Aggregate all the elements of the RDD by: 
# MAGIC   - applying a user function to combine elements with user-supplied objects, 
# MAGIC   - then combining those user-defined results via a second user function, 
# MAGIC   - and finally returning a result to the driver.
# MAGIC   
# MAGIC ![](http://i.imgur.com/7MLnYeh.png)

# COMMAND ----------

seqOp = lambda data, item: (data[0] + [item], data[1] + item)
combOp = lambda d1, d2: (d1[0] + d2[0], d1[1] + d2[1])

x = sc.parallelize([1,2,3,4])

y = x.aggregate(([], 0), seqOp, combOp)

print(y)

# COMMAND ----------

# MAGIC %md ####**Try it!** Can you use .aggregate to collect the inputs into a plain list -- so that the output of your .aggregate is just like that of .collect? How about producing a plain total, just like .sum? What does that tell you about the amount of data returned from .aggregate?

# COMMAND ----------

x = sc.parallelize([1,2,3,4])

#define appropriate seqOp and combOp
seqOp = lambda aList, n: aList + [n] 
combOp = lambda l1, l2 : l1+l2

y = x.aggregate([], seqOp, combOp) #add correct parameters

# these two lines should produce the same thing
print(x.collect())
print(y)

opSum = lambda x,y:x+y

print(x.aggregate(0, opSum, opSum)) # for sum, the adder/combiner can be the same

# COMMAND ----------

# MAGIC %md ### Max, Min, Sum, Mean, Variance, Stdev
# MAGIC 
# MAGIC Action / To Driver: Compute the respective function (maximum value, minimum value, sum, mean, variance, or standard deviation) from a numeric RDD
# MAGIC 
# MAGIC ![](http://i.imgur.com/HUCtib1.png)

# COMMAND ----------

x = sc.parallelize([2,4,1])
print(x.collect())
print(x.max(), x.min(), x.sum(), x.mean(), x.variance(), x.stdev())

# COMMAND ----------

# MAGIC %md ### CountByKey
# MAGIC 
# MAGIC Action / To Driver: Return a map of keys and counts of their occurrences in the RDD
# MAGIC 
# MAGIC ![](http://i.imgur.com/jvQTGv6.png)

# COMMAND ----------

x = sc.parallelize([('J', 'James'), ('F','Fred'), 
                    ('A','Anna'), ('J','John')])

y = x.countByKey()
print(y)

# COMMAND ----------

# MAGIC %md ### SaveAsTextFile
# MAGIC 
# MAGIC Action / Distributed: Save the RDD to the filesystem indicated in the path
# MAGIC 
# MAGIC ![](http://i.imgur.com/Tb2Q9mG.png)

# COMMAND ----------

dbutils.fs.rm("/temp/demo", True)
x = sc.parallelize([2,4,1])
x.saveAsTextFile("/temp/demo")

y = sc.textFile("/temp/demo")
print(y.collect())


# COMMAND ----------

# MAGIC %md ### We hope this was fun! Go crunch some data!