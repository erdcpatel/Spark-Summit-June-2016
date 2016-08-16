// Databricks notebook source exported at Wed, 1 Jun 2016 20:13:07 UTC
// MAGIC %md ##Please run this notebook to mount the datasets used for the labs

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC To get access to the datasets used in your class, you must:
// MAGIC 
// MAGIC * Have an AWS account
// MAGIC * Have generated an access key and secret key
// MAGIC * Have those keys handy
// MAGIC * Run this notebook
// MAGIC 
// MAGIC We're using your keys solely for authentication purposes. We will not be storing the datasets in your S3 account.

// COMMAND ----------

// ReadOnly keys
val YourAccessKey = "AKIAJBRYNXGHORDHZB4A"
val YourSecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"

// COMMAND ----------

import java.net.URLEncoder
val AccessKey = YourAccessKey
// URL Encode the Secret Key as that can contain "/" and other characters.
val SecretKey = URLEncoder.encode(YourSecretKey, "UTF-8")
val AwsBucketName = "db-wikipedia-readonly-use"
val MountName = "/mnt/wikipedia-readonly/"

dbutils.fs.mount(s"s3a://$AccessKey:$SecretKey@$AwsBucketName", s"$MountName")

display(dbutils.fs.ls("/mnt/wikipedia-readonly/"))

// COMMAND ----------


