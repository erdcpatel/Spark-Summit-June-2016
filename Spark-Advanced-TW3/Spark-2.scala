// Databricks notebook source exported at Wed, 8 Jun 2016 17:25:35 UTC
// MAGIC %md #Modern Spark DataFrame/Dataset: 
// MAGIC ##Frequent Questions, Answers, and a Look Behind the Scenes

// COMMAND ----------

// MAGIC %sh wget http://media.mongodb.org/zips.json

// COMMAND ----------

// MAGIC %sh pwd

// COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/zips.json", "dbfs:/FileStore/zips.json")

// COMMAND ----------

val DATAPATH = "dbfs:/FileStore/zips.json"
sqlContext.read.json(DATAPATH).registerTempTable("zip")

// COMMAND ----------

spark.read.json(DATAPATH).createOrReplaceTempView("zip")

// COMMAND ----------

spark.table("zip").withColumnRenamed("_id", "zip").createOrReplaceTempView("zip")

// COMMAND ----------

// MAGIC %md What are the most populous cities in Illinois, and how many postcodes do they have?

// COMMAND ----------

// MAGIC %sql SELECT COUNT(zip), SUM(pop), city FROM zip WHERE state = 'IL'
// MAGIC   GROUP BY city 
// MAGIC   ORDER BY SUM(pop) DESC 
// MAGIC   LIMIT 10

// COMMAND ----------

// MAGIC %md #### How bad can that be with RDDs?
// MAGIC 
// MAGIC __Well ... let's have a look!__

// COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/zip.csv", true)
sqlContext.table("zip").select("zip", "city", "state", "pop").write.format("csv").save("dbfs:/FileStore/zip.csv")

// COMMAND ----------

val zipRDD = sc.textFile("dbfs:/FileStore/zip.csv").map(line => {
  val fields = line.split(",")
  (fields(0), fields(1), fields(2), fields(3).toInt) //zip, city, state, pop
})

println("CITY (# POSTCODES , POPULATION)")
zipRDD.filter(_._3 == "IL")
  .map(record => (record._2, (1, record._4)))
  .reduceByKey( (a,b) => (a._1 + b._1, a._2 + b._2) )
  .sortBy(- _._2._2)
  .take(10).foreach(println)

// COMMAND ----------

// MAGIC %md #### ARGH!!

// COMMAND ----------

// MAGIC %md ##### Is this the best way to write this query?
// MAGIC 
// MAGIC It's reasonable, but ... would Spark fix it if, say, wrote this (worse) version:

// COMMAND ----------

println("CITY (# POSTCODES , POPULATION)")
zipRDD
  .map(record => ((record._2, record._3), (1, record._4)))
  .reduceByKey( (a,b) => (a._1 + b._1, a._2 + b._2) )
  .sortBy(- _._2._2)
  .filter(_._1._2 == "IL")
  .take(10).foreach(println)

// COMMAND ----------

// MAGIC %md #### The answer is "no" -- let's go take a look at the Job UI ...

// COMMAND ----------

// MAGIC %md ## ... ok ... on to DataFrame and Dataset details!

// COMMAND ----------

// MAGIC %md ### Dataset is just a generalization of DataFrame
// MAGIC 
// MAGIC So it's easier to use in even more situations

// COMMAND ----------

import org.apache.spark.sql._

classOf[DataFrame] == classOf[Dataset[_]]


// COMMAND ----------

// MAGIC %md Dataset strong typing is "virtual" -- it's just a view that can be applied when you want it:

// COMMAND ----------

val ds = sqlContext.range(3)
ds.printSchema

// COMMAND ----------

ds.as[String]

// COMMAND ----------

// MAGIC %md Note the Scala type changes ... the underlying schema doesn't. What is "bigint"? Underlying types are language-neutral, database-flavored types (think HIVE or SQL).

// COMMAND ----------

// MAGIC %md Asking for a Dataframe gets you Rows:

// COMMAND ----------

ds.toDF.collect

// COMMAND ----------

// MAGIC %md Language-neutral, you say? Interesting...

// COMMAND ----------

ds.createOrReplaceTempView("numbers")

// COMMAND ----------

// MAGIC %python sqlContext.table("numbers").printSchema()

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC for line in sqlContext.table("numbers").collect():
// MAGIC   print line.id

// COMMAND ----------

// MAGIC %md Ok, how would this work with a more complicated type?

// COMMAND ----------

case class Zip(zip:String, city:String, loc:Array[Double], pop:Long, state:String) {
  val latChicago = 41.8781136
  val lonChicago = -87.6297982
  
  def nearChicago = {
    // pretend GIS / business-domain logic:
    math.sqrt(math.pow(loc(0)-lonChicago, 2) + math.pow(loc(1)-latChicago, 2) ) < 1
  }
}

sqlContext.table("zip").as[Zip].first

// COMMAND ----------

val ds = sqlContext.table("zip").as[Zip]
ds.first.loc

// COMMAND ----------

ds.groupBy('state).count.orderBy('state).show

// COMMAND ----------

// MAGIC %md "Complex" or otherwise restricted business-domain logic can easily be used:

// COMMAND ----------

sqlContext.read.json(DATAPATH).withColumnRenamed("_id","zip").as[Zip].filter(_.nearChicago).show

// COMMAND ----------

// MAGIC %md __Query Optimization__

// COMMAND ----------

import org.apache.spark.sql.functions._

ds.filter('pop > 10000).select(lower('city)).filter('state === "RI").collect

// COMMAND ----------

ds.filter('pop > 10000).select(lower('city)).filter('state === "RI").explain(true)

// COMMAND ----------

sqlContext.read.json(DATAPATH).withColumnRenamed("_id","zip").as[Zip].filter(_.nearChicago).explain(true)

// COMMAND ----------

// MAGIC %md __Whole-Stage CodeGen__

// COMMAND ----------

import org.apache.spark.sql.execution.debug._

sqlContext.read.json(DATAPATH).withColumnRenamed("_id","zip").as[Zip].filter(_.nearChicago).debugCodegen

// COMMAND ----------

// MAGIC %md (2.0 -- Spark Session: "spark")

// COMMAND ----------

spark.table("zip")

// COMMAND ----------

// MAGIC %md You can configure Spark to use your Hive metastore and warehouse... 
// MAGIC 
// MAGIC Where is the default Hive warehouse in Databricks?

// COMMAND ----------

display(spark.catalog.listDatabases)

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/hive_zip"))

// COMMAND ----------

// MAGIC %md __Structured Streaming Demo__

// COMMAND ----------

dbutils.fs.rm("/stream/in", true)
dbutils.fs.mkdirs("/stream/in")
dbutils.fs.rm("/stream/out", true)
dbutils.fs.mkdirs("/stream/out")
dbutils.fs.rm("/stream/ck", true)
dbutils.fs.mkdirs("/stream/ck")

import org.apache.spark.sql.types._

val schema = StructType(StructField("lastname", StringType, false) :: StructField("email", StringType, false) :: StructField("hits", IntegerType, false) :: Nil)

// COMMAND ----------

val query = spark.read.format("json").schema(schema).stream("dbfs:/stream/in/")

// COMMAND ----------

import org.apache.spark.sql.functions._

val modifiedData = query.select('lastname, lower('email) as "email", current_timestamp() as "now", 'hits)

val stream = modifiedData.write.option("checkpointLocation", "dbfs:/stream/ck/").startStream("dbfs:/stream/out/")

// COMMAND ----------

// MAGIC %sh echo -e '{"lastname":"Jones", "email":"Jones@Gmail.com", "hits": 3}\n{"lastname":"Smith", "email":"smith@GMAIL.com", "hits": 5}' > /tmp/d1.txt

// COMMAND ----------

dbutils.fs.mv("file:/tmp/d1.txt", "dbfs:/stream/in")

// COMMAND ----------

display(spark.read.parquet("dbfs:/stream/out"))

// COMMAND ----------

display(spark.read.parquet("dbfs:/stream/out").select("email", "hits").groupBy("email").sum("hits"))

// COMMAND ----------

// MAGIC %sh echo '{"lastname":"Smith", "email":"smith@GMAIL.com", "hits" : 12}' > /tmp/d2.txt

// COMMAND ----------

dbutils.fs.mv("file:/tmp/d2.txt", "dbfs:/stream/in")

// COMMAND ----------

display(spark.read.parquet("dbfs:/stream/out").select("email", "hits").groupBy("email").sum("hits"))

// COMMAND ----------

stream.stop

// COMMAND ----------


