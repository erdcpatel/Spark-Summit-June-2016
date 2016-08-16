# Databricks notebook source exported at Wed, 4 Nov 2015 20:20:36 UTC
# MAGIC %md
# MAGIC # Custom Accumulators Lab
# MAGIC 
# MAGIC In this lab, we'll build a couple custom Accumulator classes. (Well, we'll show you one; then you'll build one of your own.)
# MAGIC 
# MAGIC First, some useful imports...

# COMMAND ----------

from pyspark import *
import random

# COMMAND ----------

# MAGIC %md Let's create an accumulator that's a map. We'll use it to count words. Normally, we'd just use the typical word-count RDD transformation pattern, but this alternate approach is useful when you want to do the counting as a _side effect_ of something else you're doing.

# COMMAND ----------

# MAGIC %md We'll need a custom accumulator class. This accumulator will be a map of `String` (word) to `Int` (word count).

# COMMAND ----------

class AccumWordCounts(AccumulatorParam):
    def zero(self, initialValue):
        '''
        zero() takes an initial value and converts it (if necessary)
        into the actual initial value for the accumulator. In this case,
        we'll just accept the caller's initial value.
    
        initial - the caller's initial value.
    
        returns the actual initial value
        '''
        return {}

    def addInPlace(self, d1, d2):
        '''
        addInPlace() is responsible for taking two maps and combining
        them into one. Spark uses it to sum up the various node-specific
        instances of the accumulator.
    
        m1  the first map
        m2  the second map

        returns the combined map
        '''
        keys1 = set(d1.keys())
        keys2 = set(d2.keys())
        common_keys = keys1 & keys2
        unique1 = keys1 - common_keys
        unique2 = keys2 - common_keys
        
        # The keys that are common between both maps must have their
        # counts summed. The keys that are unique can just be copied
        # to the new map.
        common_tuples = [(k, d1[k] + d2[k]) for k in common_keys]
        unique1_tuples = [(k, d1[k]) for k in unique1]
        unique2_tuples = [(k, d2[k]) for k in unique2]
        return dict(common_tuples + unique1_tuples + unique2_tuples)

# COMMAND ----------

# MAGIC %md Next, we need to create an instance of an accumulator of this type.

# COMMAND ----------

count_map = sc.accumulator({}, AccumWordCounts())

# COMMAND ----------

# MAGIC %md Now, let's test it with a parallelized data set, which makes it easier to validate.

# COMMAND ----------

rdd = sc.parallelize(["and", "and", "then", "the", "leaves", "grass", "green", "leaves"])

# COMMAND ----------

# MAGIC %md We'll use the distributed action `foreach` to count each word. In addition, we'll convert the RDD to another RDD with upper-cased words.

# COMMAND ----------

rdd2 = rdd.map(lambda word: word.upper())
# Note that we have to add a dict here.
def update(word):
  global count_map
  count_map += {word: 1}
rdd2.foreach(update)

# COMMAND ----------

for word in rdd2.collect():
  print word

# COMMAND ----------

# MAGIC %md What's the accumulator look like?

# COMMAND ----------

counts = count_map.value
for key in sorted(counts):
  print "{0} -> {1}".format(key, counts[key])

# COMMAND ----------

# MAGIC %md ## Exercise
# MAGIC 
# MAGIC You're going to create an accumulator that can be used to keep track of unique occurrences of numbers. A `set` is a useful way to keep track of uniqueness.

# COMMAND ----------

class AccumWordCounts(AccumulatorParam):
    def zero(self, ...):
      # FILL IN
      pass

    def addInPlace(self, ...):
      # FILL IN
      pass

# COMMAND ----------

unique_numbers = sc.accumulator(set(), AccumWordCounts())

# COMMAND ----------

# Check the initial value of the accumulator.
unique_numbers.value

# COMMAND ----------

# MAGIC %md To test your accumulator, we'll use 100,000 random numbers, with some guaranteed overlap. (We'd use 1,000,000, like the Scala lab, but Python is too slow...)

# COMMAND ----------

numbers = [random.randint(0, 10000) for i in range(1, 100000)]

# COMMAND ----------

rdd = sc.parallelize(numbers, 4)

# COMMAND ----------

# MAGIC %md Here's where you need to update the accumulator.

# COMMAND ----------

def update_set(i):
  # FILL IN
  pass

rdd.foreach(...)

# COMMAND ----------

print("Random numbers: {0}\nUnique numbers: {1}".format(
  rdd.count(), len(unique_numbers.value))
)

# COMMAND ----------

