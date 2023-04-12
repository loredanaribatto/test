# Databricks notebook source
# Display songs
display(dbutils.fs.ls("/databricks-datasets/songs/"))

# COMMAND ----------

# Create table

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

# Define variables used in code below
file_path = "/databricks-datasets/songs/data-001/"
table_name = "pipeline_get_started_prepared_song_data"
checkpoint_path = "pipeline_get_started_raw_song_data"

# For purposes of this example, clear out data from previous runs. Because Auto Loader
# is intended for incremental loading, in production applications you normally won't drop
# target tables and checkpoints between runs.
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

schema = StructType(
  [
    StructField("artist_id", StringType(), True),
    StructField("artist_lat", DoubleType(), True),
    StructField("artist_long", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("end_of_fade_in", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("key_confidence", DoubleType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("release", StringType(), True),
    StructField("song_hotnes", DoubleType(), True),
    StructField("song_id", StringType(), True),
    StructField("start_of_fade_out", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", DoubleType(), True),
    StructField("time_signature_confidence", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("partial_sequence", IntegerType(), True)
  ]
)

(spark.readStream
  .format("cloudFiles")
  .schema(schema)
  .option("cloudFiles.format", "csv")
  .option("sep","\t")
  .load(file_path)
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC   artists_by_year
# MAGIC AS SELECT
# MAGIC   artist_name,
# MAGIC   year
# MAGIC FROM
# MAGIC   pipeline_get_started_prepared_song_data
# MAGIC -- Remove records where the year field isn't populated
# MAGIC WHERE
# MAGIC   year > 0;
# MAGIC 
# MAGIC -- Which artists released the most songs in each year?
# MAGIC SELECT
# MAGIC   artist_name,
# MAGIC   count(artist_name)
# MAGIC AS
# MAGIC   num_songs,
# MAGIC   year
# MAGIC FROM
# MAGIC   artists_by_year
# MAGIC GROUP BY
# MAGIC   artist_name,
# MAGIC   year
# MAGIC ORDER BY
# MAGIC   num_songs DESC,
# MAGIC   year DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find songs for your DJ list
# MAGIC  CREATE OR REPLACE VIEW
# MAGIC    danceable_songs
# MAGIC  AS SELECT
# MAGIC    artist_name,
# MAGIC    title,
# MAGIC    tempo
# MAGIC  FROM
# MAGIC    pipeline_get_started_prepared_song_data
# MAGIC  WHERE
# MAGIC    time_signature = 4
# MAGIC    AND
# MAGIC    tempo between 100 and 140;
# MAGIC 
# MAGIC  SELECT * FROM danceable_songs limit 100
