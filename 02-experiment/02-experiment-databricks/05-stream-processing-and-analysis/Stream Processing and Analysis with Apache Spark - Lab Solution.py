# Databricks notebook source
checkpoint_location_prefix= '/Volumes/workspace/default/checkpoint'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VOLUME IF EXISTS checkpoint;
# MAGIC CREATE VOLUME IF NOT EXISTS checkpoint;

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab Solution - Introduction to Spark Structured Streaming
# MAGIC
# MAGIC In this lab, you'll work with a streaming dataset containing order status updates. You'll learn how to create streaming DataFrames, perform basic transformations, and work with different streaming sinks.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Understand stream processing fundamentals
# MAGIC - Implement basic streaming operations
# MAGIC - Work with different streaming sources and sinks
# MAGIC - Apply streaming transformations and watermarking
# MAGIC - Handle late data and monitor streaming queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Processing Setup
# MAGIC
# MAGIC First, let's set up our streaming infrastructure and define our data schema.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
# Define the schema
schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_status", StringType(), True),
    StructField("status_timestamp", LongType(), True)
])

# Create a streaming DataFrame using this schema
status_stream = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .option("path", "/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/status/stream_json") \
    .load()

# Verify it's a streaming DataFrame
print(f"isStreaming: {status_stream.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Queries
# MAGIC
# MAGIC Now we will create a basic streaming query, using the memory sink which we will subsequently query using SQL.

# COMMAND ----------

# Write the results of your status_stream into a memory sink with a query name of "order_status_streaming_table", appending records to the output sink
# Stop any existing queries with the same name

for q in spark.streams.active:
    if q.name == "order_status_streaming_table":
        q.stop()

# Write to memory sink for interactive querying
memory_query = (status_stream.writeStream
    .format("memory")
    .queryName("order_status_streaming_table")
    .trigger(availableNow=True)
    .option("checkpointLocation", f'{checkpoint_location_prefix}/memory_query')
    .outputMode("append")
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_status, count(*) as cnt 
# MAGIC FROM order_status_streaming_table
# MAGIC GROUP BY order_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Transformations
# MAGIC
# MAGIC Now, you'll perform some basic transformations on the streaming data.

# COMMAND ----------

# Perform basic transformations
transformed_stream = status_stream \
    .withColumn("event_time", from_unixtime(col("status_timestamp") / 1000).cast("timestamp")) \
    .withColumn("status_description", concat(lit("Order status: "), col("order_status"))) \
    .withColumn("is_completed", col("order_status").isin("delivered", "canceled"))

# Display the transformed stream
display(transformed_stream, checkpointLocation=f'{checkpoint_location_prefix}/transformed_stream')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Controlling Processing with Triggers
# MAGIC
# MAGIC Finally, you'll use triggers to control how the stream processes data.

# COMMAND ----------

# Stop any existing queries with the same name
for q in spark.streams.active:
    if q.name == "triggered_status_updates":
        q.stop()
        
# Create a triggered streaming query
triggered_query = (status_stream \
    .withColumn("processing_time", current_timestamp())
    .writeStream
    .format("memory")
    .queryName("triggered_status_updates") 
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", f'{checkpoint_location_prefix}/triggered_query')
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the processing batches
# MAGIC SELECT 
# MAGIC   processing_time,
# MAGIC   order_status,
# MAGIC   count(*) as record_count
# MAGIC FROM triggered_status_updates
# MAGIC GROUP BY processing_time, order_status
# MAGIC ORDER BY processing_time, order_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Stream Processing Fundamentals**
# MAGIC    - Structured Streaming provides a DataFrame-based streaming API
# MAGIC    - Supports both batch and streaming processing models
# MAGIC    - Handles data consistency and fault tolerance
# MAGIC
# MAGIC 2. **Sources and Sinks**
# MAGIC    - Multiple input sources available (Rate, File, Kafka, etc.)
# MAGIC    - Various output sinks for different use cases
# MAGIC    - Memory sink useful for testing and debugging
# MAGIC
# MAGIC 3. **Data Processing**
# MAGIC    - Supports standard DataFrame operations
# MAGIC    - Windowing and watermarking for time-based processing
# MAGIC    - Aggregations and streaming joins
# MAGIC
# MAGIC 4. **Monitoring and Management**
# MAGIC    - Built-in query monitoring capabilities
# MAGIC    - Progress tracking and metrics
# MAGIC    - Late data handling strategies
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to stop the active streaming queries.

# COMMAND ----------

for query in spark.streams.active:
    query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab Solution - Window Aggregation in Spark Structured Streaming
# MAGIC
# MAGIC In this lab, you'll work with stateful operations, sliding windows, and watermarks in Spark Structured Streaming. You'll analyze streams of order and status data to derive meaningful insights.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Implement stateful aggregations and window operations
# MAGIC - Handle late data and state management
# MAGIC - Build real-time monitoring systems

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Data Sources
# MAGIC
# MAGIC First, let's set up our streaming environment with the necessary data sources.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Schemas are provided for you
orders_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("notifications", StringType(), True),
    StructField("order_id", LongType(), True),
    StructField("order_timestamp", LongType(), True)
])

status_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_status", StringType(), True),
    StructField("status_timestamp", LongType(), True)
])

# Create status streaming DataFrame
status_stream = spark.readStream \
    .format("json") \
    .schema(status_schema) \
    .option("maxFilesPerTrigger", 1) \
    .option("path", "/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/status/stream_json") \
    .load()

# Create orders streaming DataFrame
orders_stream = spark.readStream \
    .format("json") \
    .schema(orders_schema) \
    .option("maxFilesPerTrigger", 1) \
    .option("path", "/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/orders/stream_json") \
    .load()

# Add event_time column to status stream
status_events = status_stream \
    .withColumn("event_time", from_unixtime(col("status_timestamp")).cast("timestamp"))

# Verify streams are set up correctly
print(f"orders_stream is streaming: {orders_stream.isStreaming}")
print(f"status_stream is streaming: {status_stream.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stateful Operations
# MAGIC
# MAGIC Let's explore stateful operations that maintain state across micro-batches.

# COMMAND ----------

# First, clean up any existing queries with the same names
for query in spark.streams.active:
    if query.name in ["status_counts", "customer_counts"]:
        query.stop()

# Count orders by status (stateful aggregation)
status_counts = status_stream \
    .groupBy("order_status") \
    .count() \
    .orderBy(col("count").desc())

# Count orders by customer (stateful aggregation)
customer_counts = orders_stream \
    .groupBy("customer_id") \
    .count() \
    .orderBy(col("count").desc())

# Write status counts to memory
status_query = (status_counts.writeStream \
    .format("memory")
    .outputMode("complete")
    .trigger(availableNow=True)
    .option("checkpointLocation", f'{checkpoint_location_prefix}/status_query')
    .queryName("status_counts")
    .start()
)

# Write customer counts to memory
customer_query = (customer_counts.writeStream \
    .format("memory")
    .outputMode("complete")
    .trigger(availableNow=True)
    .option("checkpointLocation", f'{checkpoint_location_prefix}/status_query')
    .queryName("customer_counts")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can query these tables to see the results:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the in-memory table to see status counts
# MAGIC SELECT * FROM status_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the in-memory table to see customer counts
# MAGIC select * from customer_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sliding Window Operations
# MAGIC In this section, you'll implement sliding window aggregations on the streaming data.
# MAGIC

# COMMAND ----------

# First, clean up any existing queries with the same name
for query in spark.streams.active:
    if query.name == "sliding_windows":
        query.stop()

# Create sliding window aggregation
sliding_window_counts = status_events \
    .groupBy(
        window(col("event_time"), "3 minutes", "1 minute"),
        col("order_status")
    ) \
    .count()

# Write sliding window counts to memory
sliding_window_query = (sliding_window_counts.writeStream \
    .format("memory")
    .trigger(availableNow=True)
    .option("checkpointLocation", f'{checkpoint_location_prefix}/sliding_window_query')
    .outputMode("complete")
    .queryName("sliding_windows")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC You can query the sliding window results:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the sliding window results
# MAGIC SELECT 
# MAGIC   window.start as window_start,
# MAGIC   window.end as window_end,
# MAGIC   order_status,
# MAGIC   count
# MAGIC FROM sliding_windows
# MAGIC ORDER BY window_start, order_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Late Data Handling with Watermarks
# MAGIC Now, let's explore how to handle late-arriving data using watermarks.

# COMMAND ----------

# First, clean up any existing queries with the same names
for query in spark.streams.active:
    if query.name in ["windowed_with_watermark", "joined_with_watermark"]:
        query.stop()

# Add watermark to status events
status_with_watermark = status_events \
    .withWatermark("event_time", "5 minutes")

# Windows with watermark
watermarked_windows = status_with_watermark \
    .groupBy(
        window(col("event_time"), "3 minutes", "1 minute"),
        col("order_status")
    ) \
    .count()

# Write to memory
watermark_query = watermarked_windows.writeStream \
    .format("memory") \
    .outputMode("complete") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", f'{checkpoint_location_prefix}/watermark_query') \
    .queryName("windowed_with_watermark") \
    .start()

# Prepare orders stream with event_time for join
orders_with_time = orders_stream \
    .withColumn("order_time", from_unixtime(col("order_timestamp")).cast("timestamp")) \
    .withWatermark("order_time", "5 minutes")

# Join with watermarks to limit state
watermarked_join = orders_with_time \
    .join(
        status_with_watermark,
        "order_id",
        "inner"
    )

# Write to memory
join_query = watermarked_join.writeStream \
    .format("memory") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", f'{checkpoint_location_prefix}/join_query') \
    .queryName("joined_with_watermark") \
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC Query the results:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   window.start as window_start,
# MAGIC   window.end as window_end,
# MAGIC   order_status,
# MAGIC   count
# MAGIC FROM windowed_with_watermark
# MAGIC ORDER BY window_start, order_status

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_id, 
# MAGIC   customer_id, 
# MAGIC   order_status,
# MAGIC   notifications,
# MAGIC   order_time,
# MAGIC   event_time as status_time
# MAGIC FROM joined_with_watermark
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to stop the active streaming queries.

# COMMAND ----------

for query in spark.streams.active:
    query.stop()
