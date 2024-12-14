# Databricks notebook source
# MAGIC %pip install google-api-python-client

# COMMAND ----------

# MAGIC %run ./countries

# COMMAND ----------

import random

random_country = random.choice(countries)
keyword = random_country[1]
print(keyword)

maxResults = 50
file_name = f"search_{keyword}_video_results"

# COMMAND ----------

from googleapiclient.discovery import build
import json
import pyspark 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, explode
from pyspark.sql.functions import lit

def extract_video_details_by_keyword(keyword, maxResults):
    youtube = build('youtube', 'v3', developerKey='AIzaSyCHVzPc8wMUpSPpR6G33kp4D76_Hkqu0gI')
    request = youtube.search().list(
        q = keyword,
        order = "relevance",
        part="snippet",
        maxResults = maxResults
    )
    response = request.execute()
    return response

def loadRawData(data, file_name):
    raw_data = json.dumps(data, indent=4)

    with open(f"/dbfs/FileStore/tables/{file_name}.json", "w") as file:   
        file.write(raw_data)
        
    file_path = f"/FileStore/tables/{file_name}.json"
    return file_path

def transformData(keyword, file_path):
    spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
    raw_results_df = spark.read.option("multiline","true").json(file_path)
    df_with_keyword = raw_results_df.withColumn("keyword", lit(keyword).cast("string"))
    df_exploded = df_with_keyword.withColumn("item", explode(col("items")))
    
    df_exploded_flattened = df_exploded.select(
        col("keyword"),
        col("etag"),
        col("nextPageToken"),
        col("item.id.videoId").alias("videoId"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.description").alias("description"),
        col("item.snippet.channelTitle").alias("channelTitle"),
        col("item.snippet.publishedAt").cast("timestamp").alias("publishedAt")
    )
    df_exploded_flattened.show()

    return df_exploded_flattened

def loadIntoDeltaTable(result_df):
    existing_table = spark.table("delta_tables.youtube_video_results_by_keyword")
    if existing_table.isEmpty():
        result_df.write.format("delta").mode("append").saveAsTable("delta_tables.youtube_video_results_by_keyword")
    
    else:
        df_no_duplicated = result_df.join(existing_table, "videoId", "left_anti")
        df_no_duplicated.write.format("delta").mode("append").saveAsTable("delta_tables.youtube_video_results_by_keyword")



details = extract_video_details_by_keyword(keyword, maxResults)
file_path = loadRawData(details, file_name)
result_df = transformData(keyword, file_path)
loadIntoDeltaTable(result_df)


# COMMAND ----------

# %sql
# create database if not exists delta_tables;


# COMMAND ----------


# %sql
# create table if not exists delta_tables.youtube_video_results_by_keyword(
#   keyword string,
#   etag string,
#   nextPageToken string,
#   videoId string,
#   title string,
#   description string,
#   channelTitle string, 
#   publishedAt timestamp
#   )
