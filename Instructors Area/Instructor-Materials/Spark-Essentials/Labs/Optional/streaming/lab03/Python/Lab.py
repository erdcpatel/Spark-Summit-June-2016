# Databricks notebook source exported at Sat, 19 Sep 2015 14:06:06 UTC
# MAGIC %md
# MAGIC # Log File Streaming Lab
# MAGIC 
# MAGIC In this lab, we'll be reading (fake) log data from a stream.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## WARNING
# MAGIC 
# MAGIC This lab does not currently work. There are, apparently, issues with the Python streaming library.

# COMMAND ----------

from pyspark import *
from pyspark.streaming import *
from pyspark.sql import *
import random
from datetime import datetime

def now_as_millis():
  import time
  import calendar
  return calendar.timegm(time.gmtime())

assert int(sc.version.replace(".", "")) >= 140, "Spark 1.4.0 or greater is required."

# COMMAND ----------

BATCH_INTERVAL_SECONDS = 15
OUTPUT_DIR = "dbfs:/tmp/streaming/logs/{0}".format(random.randint(1, 1000))
OUTPUT_FILE = "{0}/{1}.parquet".format(OUTPUT_DIR, now_ms)
CHECKPOINT_DIR = "{0}/checkpoint".format(OUTPUT_DIR)

print "Using: Batch interval of {0} second(s)".format(BATCH_INTERVAL_SECONDS)
print '       Output directory: "{0}"'.format(OUTPUT_DIR)
print '       Output file:      "{0}"'.format(OUTPUT_FILE)
print '       Checkpoint dir:   "{0}"'.format(CHECKPOINT_DIR)


# COMMAND ----------

# MAGIC %md
# MAGIC We need to pull in some configuration data.

# COMMAND ----------

# MAGIC %run "/Labs/streaming/lab03/Python/Configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create our Streaming Process
# MAGIC 
# MAGIC We're going to connect to a TCP server that serves log messages, one per line. We'll read the messages and write them to a Parquet file.
# MAGIC 
# MAGIC We're going to be using a complicated-looking regular expression to take each log message apart:
# MAGIC 
# MAGIC ```
# MAGIC ^\[(.*)\]\s+\(([^)]+)\)\s+(.*)$
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC The following class contains the code for our stream reader.

# COMMAND ----------

import re
from collections import namedtuple

class Runner(object):

  def __init__(self):
    self._ssc = None
    
  def stop(self):
    """
    Stop the streaming context.
    """
    if self._ssc:
      self._ssc.stop(stopSparkContext=False, stopGraceFully=True)
      self._ssc = None
      print "Stopped active streaming context."
  
  def start(self):
    """
    Start the streaming context
    """
    # NOTE: StreamingContext.getOrCreate() exists in the Python API, but it currently
    # throws errors.
    #self._ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, Runner._create_streaming_context)
    self._ssc = Runner._create_streaming_context()
    print "Starting {0}".format(self._ssc)
    self._ssc.start()
    print "Started streaming context."
  
  def restart(self):
    """
    Restart the streaming context.
    """
    self.stop()
    self.start()
   
  @classmethod
  def _create_streaming_context(cls):
    LogMessage = namedtuple('LogMessage', ['message_type', 'timestamp', 'message'])

    LOG_LINE_PATTERN = re.compile(r'^\[(.*)\]\s+\(([^)]+)\)\s+(.*)$')
    TIMESTAMP_FORMAT = '%Y/%m/%d %H:%M:%S.%f'
    
    ssc = StreamingContext(sc, BATCH_INTERVAL_SECONDS)
    ds = ssc.socketTextStream(LOG_SERVER_HOST, LOG_SERVER_PORT)

    # What we're getting is a stream of log lines. As each RDD is created by
    # Spark Streaming, have the stream create a new RDD that extracts just
    # the error messages, dropping the rest.
    def parse_log_line(line):
      log_message = None
      try:
        m = LOG_LINE_PATTERN.search(line)
        if m:
          timestamp = datetime.strptime(TIMESTAMP_FORMAT, m.group(1))
          log_message = LogMessage(m.group(2), timestamp, m.group(3))
      except:
        pass

      return log_message

    #keep_logs_stream = ds.map(parse_log_line).filter(lambda line: line is not None)

    # Now, each RDD created by the stream will be converted to something
    # we can save to a persistent store that can easily be read, later,
    # into a DataFrame.
    def save_rdd(rdd):
      print "*** processing rdd. rdd.isEmpty={0}".format(rdd.isEmpty())
      #if not rdd.isEmpty():
        #df = sqlContext.createDataFrame(rdd)
        #df.write.mode("append").parquet(OUTPUT_FILE)
      rdd.saveAsTextFile("{0}-{1}".format(OUTPUT_DIR, now_as_millis()))

    #keep_logs_stream.foreachRDD(save_rdd)
    ds.foreachRDD(save_rdd)

    print "--- ssc={0}".format(ssc)
    return ssc  

# COMMAND ----------

dbutils.fs.rm(OUTPUT_DIR, recurse=True)
dbutils.fs.mkdirs(OUTPUT_DIR)
runner = Runner()
runner.stop()
runner.restart()


# COMMAND ----------

# Is there anything in the output directory?
display( dbutils.fs.ls(OUTPUT_DIR) )


# COMMAND ----------

# MAGIC %md
# MAGIC Let the stream run for awhile. Then, execute the following cell to stop the stream.

# COMMAND ----------

runner.stop()

# COMMAND ----------

# MAGIC %md Okay, now let's take a look at what we have in the Parquet file. **NOTE**: This next command may fail periodically if the stream is still running.

# COMMAND ----------

val df = sqlContext.read.parquet(OUTPUT_FILE)

# COMMAND ----------

# MAGIC %md How many messages did we pull down?

# COMMAND ----------

df.count

# COMMAND ----------

display(df)

# COMMAND ----------

df.registerTempTable("messages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC How many messages of each type (ERROR, INFO, WARN) are there?
# MAGIC 
# MAGIC **HINT**: Use your DataFrame knowledge.

# COMMAND ----------

# MAGIC %sql select messageType, count(messageType) from messages group by messageType

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC Modify the `Runner`, above, to keep only the ERROR messages.

# COMMAND ----------



# COMMAND ----------

