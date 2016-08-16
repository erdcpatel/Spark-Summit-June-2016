# Databricks notebook source exported at Mon, 10 Aug 2015 15:30:38 UTC
# MAGIC %md
# MAGIC # Solutions for RDD Lab (Python)

# COMMAND ----------

# MAGIC %md ## Exercise 1

# COMMAND ----------

CrimeData = namedtuple('CrimeData', ['ccn', 'report_time', 'shift', 'offense', 'method'])

def map_line(line):
  columns = line.split(",")[:5]
  return CrimeData(*columns)

data_rdd = no_header_rdd.map(map_line)
pprint(data_rdd.take(10))

# COMMAND ----------

# MAGIC %md ## Exercise 2

# COMMAND ----------

grouped_by_offense_rdd = data_rdd.groupBy(lambda data: data.offense)

# What does this return? You'll need to know for the next step.
print grouped_by_offense_rdd.take(10)


# COMMAND ----------

# MAGIC %md ## Exercise 3

# COMMAND ----------

# Here's one way to do it:

offense_counts = grouped_by_offense_rdd.map(lambda g: (g[0], len(g[1]))).collect()
for offense, count in offense_counts:
  print "{0:30s} {1:d}".format(offense, count)

# COMMAND ----------

# But here's a better way:

offense_counts = data_rdd.map(lambda item: (item.offense, item)).countByKey()
for offense, counts in offense_counts.items():
  print "{0:30s} {1:d}".format(offense, counts)

# COMMAND ----------

# MAGIC %md ## Exercise 4

# COMMAND ----------

print base_rdd.getNumPartitions()
print grouped_by_offense_rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md ## Exercise 5

# COMMAND ----------

# It's okay to cache the base RDD. It's not very large. Proof:
print "Count of lines: {0}".format(base_rdd.count())
totalChars = base_rdd.map(lambda line: len(line)).reduce(lambda a, b: a + b)
print "Count of characters (Unicode): {0}".format(totalChars)

# They're all about the same size, since we didn't filter any data out. However,
# since we're mostly working with the `data_rdd`, that's the one to cache.
data_rdd.cache()

# COMMAND ----------

# MAGIC %md ## Exercise 6

# COMMAND ----------

# SOLUTION
result_rdd1 = data_rdd.filter(lambda item: item.offense == 'HOMICIDE')\
                      .map(lambda item: (item.method, 1))\
                      .reduceByKey(lambda i, j: i + j)

for method, count in result_rdd1.collect():
  print "{0:10s} {1:d}".format(method, count)

# COMMAND ----------

# MAGIC %md ## Exercise 7

# COMMAND ----------

# There are quite a few ways to solve this one, but here's a straightforward (and relatively fast) one.
print (data_rdd.map(lambda item: (item.shift, 1))
               .reduceByKey(lambda c1, c2: c1 + c2)
               .max(key=lambda t: t[1]))


# COMMAND ----------

