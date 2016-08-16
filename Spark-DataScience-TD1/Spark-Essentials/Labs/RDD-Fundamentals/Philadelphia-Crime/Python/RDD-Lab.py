# Databricks notebook source exported at Thu, 19 Nov 2015 23:07:30 UTC
# MAGIC %md
# MAGIC # RDD Lab (Python)
# MAGIC 
# MAGIC In this lab, we'll explore some of the RDD concepts we've discussed. We'll be using a data set consisting of reported crimes in Philadelphia in 2013. We'll use this data to explore some RDD transitions and actions.
# MAGIC 
# MAGIC ## Exercises and Solutions
# MAGIC 
# MAGIC This notebook contains a number of exercises. Use the 
# MAGIC <a href="http://spark.apache.org/docs/1.6.1/api/python/pyspark.html#pyspark.RDD" target="_blank">RDD API documentation</a>
# MAGIC to look up transformations and actions. If, at any point, you're struggling with the solution to an exercise, feel free to look in the **Solutions** notebook (in the same folder as this lab).
# MAGIC 
# MAGIC ## Let's get started.
# MAGIC 
# MAGIC First, let's import a couple things we'll need.

# COMMAND ----------

from collections import namedtuple
from pprint import pprint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the data
# MAGIC 
# MAGIC The next step is to load the data. Run the following cell to create an RDD containing the data.

# COMMAND ----------

base_rdd = sc.textFile("dbfs:/mnt/training/philadelphia-crime-data-2015-ytd.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Question 1
# MAGIC 
# MAGIC Does the RDD _actually_ contain the data right now?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the data
# MAGIC 
# MAGIC Let's take a look at some of the data.

# COMMAND ----------

base_rdd.take(10)

# COMMAND ----------

# MAGIC %md Okay, there's a header. We'll need to remove that. But, since the file will be split into partitions, we can't just drop the first item. Let's figure out another way to do it.

# COMMAND ----------

no_header_rdd = base_rdd.filter(lambda line: 'DC_DIST' not in line)

# COMMAND ----------

no_header_rdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata
# MAGIC 
# MAGIC According to the Open Data Philly site, here's what some of those fields actually mean:
# MAGIC 
# MAGIC Key attribute field names and descriptions
# MAGIC 
# MAGIC * `DC_DIST` (integer): District number
# MAGIC * `SECTOR` (integer): Sector or PSA Number
# MAGIC * `DISPATCH_DATE` (date string): Date of Incident (modified from original data)
# MAGIC * `DISPATCH_TIME` (time string): Time of Incident (modified from original data)
# MAGIC * `DC_KEY`: (text): Unique ID of each crime
# MAGIC * `UCR_General` (integer): Rounded Crime Code
# MAGIC * `TEXT_GENERAL_CODE` (string): Human-readable Crime Code
# MAGIC * `OBJECTID` (integer): Unique row ID
# MAGIC * `POINT_X` (decimal): Latitude where crime occurred
# MAGIC * `POINT_Y` (decimal): Longitude where crime occurred

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 1
# MAGIC 
# MAGIC Let's make things a little easier to handle, by converting the `no_header_rdd` to an RDD containing Python objects.
# MAGIC 
# MAGIC **TO DO**
# MAGIC 
# MAGIC * Split each line into its individual cells.
# MAGIC * Map the RDD into another RDD of appropriate `namedtuple` objects.
# MAGIC * You'll have to decide which fields from the data best map onto the fields of the case class.

# COMMAND ----------

# Replace the <FILL-IN> sections with appropriate code.

# TAKE NOTE: We are deliberately only keeping the first five fields of
# each line, since that's all we're using in this lab. There's no sense
# in dragging around more data than we need.
CrimeData = namedtuple('CrimeData', ['date_string', 'time_string', 'offense', 'latitude', 'longitude'])

def map_line(line):
  columns = << FILL THIS IN >>
  return << FILL THIS IN >>

data_rdd = no_header_rdd.map(map_line)
print data_rdd.take(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 2
# MAGIC 
# MAGIC Next, group the data by type of crime (the "OFFENSE" column).

# COMMAND ----------

grouped_by_offense_rdd = data_rdd.<< FILL THIS IN >>

# What does this return? You'll need to know for the next step.
print grouped_by_offense_rdd.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL
# MAGIC There's some junk in our data. Let's clean it up a bit.

# COMMAND ----------

import re

BAD_OFFENSE_RE = re.compile(r'^\d+$')

def clean_offense(d):
  d = CrimeData(date_string=d.date_string, 
                time_string=d.time_string,
                offense=d.offense.replace('"', '').strip(),
                latitude=d.latitude,
                longitude=d.longitude)
  return d
cleaned_rdd = data_rdd.map(clean_offense).filter(lambda d: BAD_OFFENSE_RE.search(d.offense) is None)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, copy the `groupBy` logic, above, but change it to run against the `cleanedRDD`.

# COMMAND ----------

grouped_by_offense_rdd2 = cleaned_rdd.groupBy(<< FILL THIS IN >>)


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 3
# MAGIC Next, create an RDD that counts the number of each offense. How many murders were there in 2013? How many assaults with a dangerous weapon?

# COMMAND ----------

offense_counts = << FILL THIS IN >>
for offense, count in << FILL THIS IN >>:
  print "{0:30s} {1:d}".format(offense, count)

# COMMAND ----------

# MAGIC %md ### Question
# MAGIC 
# MAGIC Run the following cell. Can you explain what happened? Is `collectAsMap()` a _transformation_ or an _action_?

# COMMAND ----------

grouped_by_offense_rdd.collectAsMap()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 4
# MAGIC 
# MAGIC How many partitions does the base RDD have? What about the `grouped_by_offense` RDD? How can you find out?
# MAGIC 
# MAGIC **Hint**: Check the [API documentation](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD).

# COMMAND ----------

print base_rdd.<< FILL THIS IN >>
print grouped_by_offense_rdd2.<< FILL THIS IN >>

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 5
# MAGIC 
# MAGIC Since we're continually playing around with this data, we might as well cache it, to speed things up.
# MAGIC 
# MAGIC **Question**: Which RDD should you cache? 
# MAGIC 
# MAGIC 1. `base_rdd`
# MAGIC 2. `no_header_rdd`
# MAGIC 3. `data_rdd`
# MAGIC 4. `cleaned_rdd`
# MAGIC 5. `grouped_by_offense_rdd2`
# MAGIC 6. None of them, because they're all still too big.
# MAGIC 7. It doesn't really matter.

# COMMAND ----------

<FILL-IN>.cache()

# COMMAND ----------

# MAGIC %md ### Exercise 6
# MAGIC 
# MAGIC Display the number of homicides.

# COMMAND ----------

result_rdd1 = data_rdd.<FILL-IN>
print result_rdd1.collect()

# BONUS: Make the output look better, using a for loop or a list comprehension.

# COMMAND ----------

result_rdd1 = cleaned_rdd.filter(lambda d: "homicide" in d.offense.lower()).map(lambda d: (d.offense, 1)).reduceByKey(lambda a, b: a + b)

for method, count in result_rdd1.collect():
  print "{0} {1:d}".format(method, count)

# COMMAND ----------

# MAGIC %md ### Demonstration
# MAGIC 
# MAGIC Let's plot murders by month. DataFrames are useful for this one.

# COMMAND ----------

# MAGIC %md To do this property, we'll need to parse the dates. That will require knowing their format. A quick sampling of the data will help.

# COMMAND ----------

cleaned_rdd.map(lambda item: item.date_string).take(30)

# COMMAND ----------

# MAGIC %md Okay. We can now create a [`strptime()` format string](https://docs.python.org/2.7/library/datetime.html?highlight=strptime#datetime.datetime.strptime), allowing us to use the `datetime.datetime` Python class to parse those strings into actual datetime values.

# COMMAND ----------

from datetime import datetime
date_format = "%Y-%m-%d"
time_format = "%H:%M:%S"


# COMMAND ----------

# MAGIC %md Now, we can create the data frame. We'll start with the `no_header_rdd` and map it slightly differently than we did to create `data_rdd`:

# COMMAND ----------

from pyspark.sql.types import *
from decimal import Decimal

CrimeData2 = namedtuple('CrimeData2', ['date', 'time', 'offense', 'latitude', 'longitude'])

def parse_date_time(fmt, s, split_on=None):
  if split_on:
    s = s.split(split_on)[0]
  try:
    return datetime.strptime(s, fmt)
  except:
    return None
  
def parse_lat_long(s):
  try:
    return Decimal(s)
  except:
    return None
  
def make_row(line):
  cols = line.split(",")
  return CrimeData2(date      = parse_date_time(date_format, cols[10]),
                    time      = parse_date_time(time_format, cols[11], split_on="."),
                    offense   = cols[6],
                    latitude  = parse_lat_long(cols[7]),
                    longitude = parse_lat_long(cols[8]))

def clean_offense2(d):
  d = CrimeData2(date=d.date, 
                 time=d.time,
                 offense=d.offense.replace('"', '').strip(),
                 latitude=d.latitude,
                 longitude=d.longitude)
  return d

df = (no_header_rdd.map(make_row).
      map(clean_offense2).
      filter(lambda d: BAD_OFFENSE_RE.search(d.offense) is None).
      toDF())


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md Let's use a user-defined function (something supported by DataFrames and Spark SQL) to give us a `month` function we can use to extract just the month part of a `Timestamp`.

# COMMAND ----------

from pyspark.sql.functions import *
display( 
  df.filter(lower(df['offense']).like('%homicide%'))
    .select(month(df['date']).alias("month"), df['offense'])
    .groupBy('month').count()
)

# COMMAND ----------

# MAGIC %md What about all crimes per month?

# COMMAND ----------

display(
  df.select(month(df['date']).alias('month'))
    .groupBy('month').count()
)

# COMMAND ----------

# MAGIC %md We can also plot the frequency of crimes by hour of day.

# COMMAND ----------

display(
  df.select(hour(df["time"]).alias("hour"), df["offense"]).groupBy("hour").count()
)

# COMMAND ----------

