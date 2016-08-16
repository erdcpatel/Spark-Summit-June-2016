// Databricks notebook source exported at Sat, 25 Jul 2015 02:38:15 UTC
// MAGIC %md ## Solution
// MAGIC 
// MAGIC Just cut and paste the function, below, into your lab.

// COMMAND ----------

def topFemaleNamesForYear(year: Int, n: Int, df: DataFrame): DataFrame = {
  df.filter($"year" === year).
     filter($"gender" === "F").
     select("firstName", "total").
     orderBy($"total".desc, $"firstName").
     limit(n).
     select("firstName")
}