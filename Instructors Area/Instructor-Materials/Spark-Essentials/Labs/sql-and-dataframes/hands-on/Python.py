# Databricks notebook source exported at Tue, 29 Sep 2015 00:42:22 UTC
# MAGIC %md
# MAGIC # SQL and DataFrames: Hands-on Exercises (Python)
# MAGIC 
# MAGIC This notebook contains hands-on exercises used in conjunction with the DataFrames module. Each section corresponds to a section in the lecture. Your instructor will tell you when it's time to do each section of this notebook. You can look at the <a href="http://spark.apache.org/docs/1.6.1/api/python/pyspark.sql.html#pyspark.sql.DataFrame" target="_blank">DataFrames API documentation</a> as well.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Inference
# MAGIC 
# MAGIC In this exercise, let's explore schema inference. We're going to be using a file called `people.txt`. The data is structured, but it has no self-describing schema. And, it's not JSON, so Spark can't infer the schema automatically. Let's create an RDD and look at the first few rows of the file.

# COMMAND ----------

rdd = sc.textFile("dbfs:/mnt/training/dataframes/people.txt")
for line in rdd.take(10):
  print line

# COMMAND ----------

# MAGIC %md As you can see, each line consists of the same information about a person:
# MAGIC 
# MAGIC * first name
# MAGIC * middle name
# MAGIC * last name
# MAGIC * gender ("M" or "F")
# MAGIC * birth date, in `yyyy-mm-dd` form
# MAGIC * a salary
# MAGIC * a United States Social Security Number
# MAGIC 
# MAGIC (Before you get _too_ excited and run out to apply for a bunch of credit cards, the Social Security Numbers are all fake.)
# MAGIC 
# MAGIC Clearly, the file has a schema, but Spark can't figure out what it is.
# MAGIC 
# MAGIC Read through the following code to see how we can apply a schema to the file. Then, run it, and see what happens.

# COMMAND ----------

from datetime import datetime
from collections import namedtuple

Person = namedtuple('Person', ['first_name', 'middle_name', 'last_name', 'gender', 'birth_date', 'salary', 'ssn'])

def map_to_person(line):
  cols = line.split(":")
  return Person(first_name  = cols[0],
                middle_name = cols[1],
                last_name   = cols[2],
                gender      = cols[3],
                birth_date  = datetime.strptime(cols[4], "%Y-%m-%d"),
                salary      = int(cols[5]),
                ssn         = cols[6])
    
people_rdd = rdd.map(map_to_person)
df = people_rdd.toDF()


# COMMAND ----------

# MAGIC %md
# MAGIC **Question:** What could go wrong in the above code? How would you fix the problems?

# COMMAND ----------

# MAGIC %md Now, let's sample some of the data.

# COMMAND ----------

sampledDF = df.sample(withReplacement = False, fraction = 0.02, seed = 1887348908234L)
display(sampledDF)

# COMMAND ----------

# MAGIC %md Finally, let's run a couple SQL commands.

# COMMAND ----------

df.registerTempTable("people")

# COMMAND ----------

# MAGIC %sql SELECT * FROM people WHERE birth_date >= '1970-01-01' AND birth_date <= '1979-12-31' ORDER BY birth_date, salary

# COMMAND ----------

# MAGIC %sql SELECT concat(first_name, " ", last_name) AS name, gender, year(birth_date) AS birth_year, salary FROM people WHERE salary < 50000

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](http://i.imgur.com/RdABwEB.png) STOP HERE.
# MAGIC 
# MAGIC Let's switch back to the slides.

# COMMAND ----------

# MAGIC %md
# MAGIC ## select and filter (and a couple more)
# MAGIC 
# MAGIC We've now seen `printSchema()`, `show()`, `select()` and `filter()`. Let's take them for a test drive.
# MAGIC 
# MAGIC First, let's look at the schema.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md Now, let's look at `show()`.

# COMMAND ----------

df.show() # show the first 20 rows of the DataFrame

# COMMAND ----------

# MAGIC %md `show()` is a good way to get a quick feel for your data. Of course, in a Databricks notebook, the `display()` helper is better. However, if you're using `spark-shell`, the `display()` helper isn't available.

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's look at `select()`. Run the following cell. What does it return?

# COMMAND ----------

df.select(df["first_name"], df["last_name"], df["gender"])

# COMMAND ----------

# MAGIC %md
# MAGIC **Remember**: Transformations are _lazy_. The `select()` method is a transformation.
# MAGIC 
# MAGIC All right. Let's look at result of a `select()` call.

# COMMAND ----------

df.select(df["first_name"], df["last_name"], df["gender"]).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, let's take a look at `filter()`, which can be used to filter data _out_ of a data set.
# MAGIC 
# MAGIC **Question**: What the does the following code actually do?

# COMMAND ----------

df.filter(df["gender"] == "M")

# COMMAND ----------

# MAGIC %md If you're familiar with the DataFrames Scala API, you'll notice that we use double-equals (`==`) in that comparison, not the triple-equals (`===`) we use in Scala. If you switch between languages, keep that in mind.
# MAGIC 
# MAGIC `filter()`, like `select()`, is a transformation: It's _lazy_.
# MAGIC 
# MAGIC Let's try something a little more complicated. Let's combine two `filter()` operations with a `select()`, displaying the results.

# COMMAND ----------

df2 = df.filter(df["gender"] == "M").filter(df["salary"] > 100000).select(df["first_name"], df["last_name"], df["salary"])
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](http://i.imgur.com/RdABwEB.png) STOP HERE.
# MAGIC 
# MAGIC Let's switch back to the slides.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## orderBy, groupBy and alias

# COMMAND ----------

# MAGIC %md Up in the first section of this notebook, we ran this SQL statement:
# MAGIC 
# MAGIC ```
# MAGIC SELECT * FROM people WHERE birthDate >= '1970-01-01' AND birthDate <= '1979-12-31' ORDER BY birthDate, salary
# MAGIC ```
# MAGIC 
# MAGIC Let's try that same query with the programmatic DataFrames API.

# COMMAND ----------

display(
  df.filter(df["birth_date"] >= "1970-01-01").filter(df["birth_date"] <= "1979-12-31").orderBy(df.birth_date, df.salary)
)

# COMMAND ----------

# MAGIC %md There are several things to note.
# MAGIC 
# MAGIC 1. We did not have to convert the date literals ("1970-01-01" and "1979-12-31") into `datetime` objects before using them in the comparisons.
# MAGIC 2. We used two different ways to specify the columns: `df["firstName"]` and `df.first_name`.
# MAGIC 
# MAGIC Let's try a `groupBy()` next.

# COMMAND ----------

display( df.groupBy(df["salary"]) )

# COMMAND ----------

# MAGIC %md Okay, that didn't work. Note that `groupBy()` returns something of type `GroupedData`, instead of a `DataFrame`. There are other methods on `GroupedData` that will convert back to a DataFrame. A useful one is `count()`.
# MAGIC 
# MAGIC **WARNING**: Don't confuse `GroupedData.count()` with `DataFrame.count()`. `GroupedData.count()` is _not_ an action. `DataFrame.count()` _is_ an action.

# COMMAND ----------

x = df.groupBy(df["salary"]).count()  # What is x?

# COMMAND ----------

display(x)

# COMMAND ----------

# MAGIC %md Let's add a filter and, while we're at it, rename the `count` column.

# COMMAND ----------

display( x.filter(x['count'] > 1).select("salary", x["count"].alias("total")) )

# COMMAND ----------

