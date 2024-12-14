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
    """
    This function uses the YouTube Data API v3 to fetch video details for a given keyword.
    
    Parameters:
    keyword (str): The search keyword (e.g., country name) used to search for YouTube videos.
    maxResults (int): The maximum number of video results to return (e.g., 50).
    
    Example:
    response = extract_video_details_by_keyword('USA', 50)
    
    Returns:
    dict: A dictionary containing the YouTube API response with video details.
    """
    youtube = build('youtube', 'v3', developerKey='AIzaSyCHVzPc8wMUpSPpR6G33kp4D76_Hkqu0gI')
    request = youtube.search().list(
        q = keyword, # Search keyword
        order = "relevance",  # Sort results by relevance
        part="snippet", # Specify the snippet part of the response
        maxResults = maxResults # Limit the number of results
    )
    response = request.execute()
    return response

def loadRawData(data, file_name):
    """
    This function writes the raw JSON data to a file in Databricks FileStore.
    
    Parameters:
    data (dict): The raw data to be saved (usually the API response).
    file_name (str): The name of the file where data should be saved (e.g., 'video_results').
    
    Example:
    file_path = loadRawData(api_response, 'video_results_usa')
    
    Returns:
    str: The path to the saved file in Databricks FileStore.
    """
    raw_data = json.dumps(data, indent=4) # Convert the data to JSON format with indentation

    # Write the raw data to a file in Databricks FileStore
    with open(f"/dbfs/FileStore/tables/{file_name}.json", "w") as file:   
        file.write(raw_data)
        
    file_path = f"/FileStore/tables/{file_name}.json" # Path to the saved file
    return file_path

def transformData(keyword, file_path):
    """
    This function transforms the raw JSON data into a structured DataFrame using Apache Spark.
    
    Parameters:
    keyword (str): The keyword used for the YouTube search (e.g., country name).
    file_path (str): The path to the raw JSON file that needs to be transformed.
    
    Example:
    transformed_df = transformData('USA', '/FileStore/tables/video_results_usa.json')
    
    Returns:
    DataFrame: A Spark DataFrame containing the transformed data.
    """
    spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

    # Read the raw JSON file into a Spark DataFrame
    raw_results_df = spark.read.option("multiline","true").json(file_path)

    # Add a new column 'keyword' to the DataFrame
    df_with_keyword = raw_results_df.withColumn("keyword", lit(keyword).cast("string"))

     # Flatten the nested 'items' array to individual rows
    df_exploded = df_with_keyword.withColumn("item", explode(col("items")))
    
    # Select specific columns and flatten them into a more accessible format
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
    df_exploded_flattened.show()  # Show the transformed DataFrame for inspection

    return df_exploded_flattened

def loadIntoDeltaTable(result_df):
    """
    This function loads the transformed data into a Delta table, ensuring no duplicate entries.
    
    Parameters:
    result_df (DataFrame): The transformed Spark DataFrame that will be loaded into the Delta table.
    
    Example:
    loadIntoDeltaTable(transformed_df)
    
    Returns:
    None: The function performs a write operation and does not return a value.
    """
    # Check if the Delta table already exists
    existing_table = spark.table("delta_tables.youtube_video_results_by_keyword")

    # If the table is empty, append new data directly to the table
    if existing_table.isEmpty():
        result_df.write.format("delta").mode("append").saveAsTable("delta_tables.youtube_video_results_by_keyword")
    
    # If the table contains existing data, remove duplicates based on 'videoId' and append new data
    else:
        df_no_duplicated = result_df.join(existing_table, "videoId", "left_anti")
        df_no_duplicated.write.format("delta").mode("append").saveAsTable("delta_tables.youtube_video_results_by_keyword")



# COMMAND ----------

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
