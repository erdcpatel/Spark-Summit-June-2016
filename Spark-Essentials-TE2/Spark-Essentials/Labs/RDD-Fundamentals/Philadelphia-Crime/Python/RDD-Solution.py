# Databricks notebook source exported at Wed, 4 Nov 2015 01:33:16 UTC
# MAGIC %md
# MAGIC # Solutions for RDD Lab (Python)

# COMMAND ----------

# MAGIC %md ## Exercise 1

# COMMAND ----------

CrimeData = namedtuple('CrimeData', ['date_string', 'time_string', 'offense', 'latitude', 'longitude'])

def map_line(line):
  cols = line.split(",")
  return CrimeData(date_string=cols[10], time_string=cols[11], offense=cols[6], latitude=cols[7], longitude=cols[8])
  
data_rdd = no_header_rdd.map(map_line)
print data_rdd.take(10)

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

offense_counts = grouped_by_offense_rdd2.map(lambda g: (g[0], len(g[1]))).collect()
for offense, count in offense_counts:
  print "{0:30s} {1:d}".format(offense, count)

# COMMAND ----------

# But here's a better way:

offense_counts = cleaned_rdd.map(lambda item: (item.offense, item)).countByKey()
for offense, counts in offense_counts.items():
  print "{0:30s} {1:d}".format(offense, counts)

# COMMAND ----------

# MAGIC %md ## Exercise 4

# COMMAND ----------

print base_rdd.getNumPartitions()
print grouped_by_offense_rdd2.getNumPartitions()

# COMMAND ----------

# MAGIC %md ## Exercise 5

# COMMAND ----------

# It's okay to cache the base RDD. It's not very large. Proof:
print "Count of lines: {0}".format(base_rdd.count())
totalChars = base_rdd.map(lambda line: len(line)).reduce(lambda a, b: a + b)
print "Count of characters (Unicode): {0}".format(totalChars)

# They're all about the same size, since we didn't filter any data out. However,
# since we're mostly working with the `cleaned_rdd`, that's the one to cache.
cleaned_rdd.cache()

# COMMAND ----------

# MAGIC %md ## Exercise 6

# COMMAND ----------

result_rdd1 = cleaned_rdd.filter(lambda d: "homicide" in d.offense.lower()).map(lambda d: (d.offense, 1)).reduceByKey(lambda a, b: a + b)

for method, count in result_rdd1.collect():
  print "{0} {1:d}".format(method, count)

# COMMAND ----------

