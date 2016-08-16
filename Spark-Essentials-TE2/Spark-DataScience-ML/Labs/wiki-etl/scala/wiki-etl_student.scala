// Databricks notebook source exported at Wed, 10 Feb 2016 23:35:51 UTC

// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Wikipedia: ETL
// MAGIC  
// MAGIC This lab explains the process that was used to obtain Wikipedia data and transform it into a more usable form for analysis in Spark.

// COMMAND ----------

// MAGIC %md
// MAGIC #### ETL Background
// MAGIC  
// MAGIC Wikipedia data from the [August 5, 2015](https://dumps.wikimedia.org/enwiki/20150805/) enwiki dump on dumps.wikimedia.org.  Using the file: `enwiki-20150805-pages-articles-multistream.xml.bz2`  The file was uncompressed, parsed to pull out the XML `<page>` tags, and parsed again to retrieve several fields as JSON.  The JSON was stored one JSON string per line so that Spark could easily load the JSON and write out a parquet file.  The resulting parquet file was downsampled to keep the dataset small for the labs.
