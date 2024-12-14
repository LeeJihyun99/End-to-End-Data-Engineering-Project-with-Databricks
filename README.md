# End-to-End-Data-Engineering-Project-with-Databricks
This project demonstrates the complete process of building an ETL data pipeline to retrieve and process YouTube video details based on a keyword related to a random country. It leverages the YouTube Data API v3 (specifically the search.list endpoint) to fetch raw JSON data containing detailed information about YouTube videos. The raw data is then transformed into a Spark DataFrame and stored in Delta Tables for efficient querying and versioning. Finally, the pipeline is automated using a Databricks job scheduler to streamline the ETL workflow.

This project is inspired by a [Medium article](https://medium.com/@georgemichaeldagogomaynard/simple-end-to-end-data-engineering-project-entirely-on-databricks-dim-833bd9d6e604). While the original article uses a different dataset, this implementation focuses on processing YouTube video information to showcase versatility and adaptability.

## Prerequisites  

To set up and run this project, you will need the following:  

- **Microsoft Azure Account**:  
  - Ensure you have a free trial for Microsoft Azure Cloud Services to avoid any charges.  
  - This project requires Azure Databricks, which is available through the Azure platform.  

> **Note:** The Databricks Community Edition is not suitable for this project because it does not include essential features such as job scheduling and workflow automation.

## Setting Up Azure Databricks  

If you are new to Azure, follow these steps to create an account and set up Azure Databricks:  

1. **Create an Azure Account**:  
   - Visit [Azure Portal](https://portal.azure.com/) to create a new account.  

2. **Search for Azure Databricks**:  
   - Once logged into the Azure Portal, search for the **'Azure Databricks'** service in the search bar.  

3. **Create a Azure Databricks workplace**:  
   - When setting this up, ensure you select **Trial (Premium - 14-Days Free DBUs)** as the pricing tier to use the free trial.  

4. **Wait for the Workspace to Be Created**:  
   - It may take a few minutes for the Azure Databricks workspace to be created.  

5. **Launch the Workspace**:  
   - Once the setup is complete, you will see a **'Launch Workspace'** button. Click on it to access the Databricks platform.  

You are now ready to proceed with setting up your ETL pipeline on Azure Databricks!  

## Step 1: Create a Cluster  
The first step is to create a cluster.  

A **cluster** is a group of virtual machines that Databricks uses to process your data. It is required because all computations, such as running notebooks or executing jobs, are performed on the cluster.  

- Set the cluster type to **Personal Compute** to ensure it is optimized for individual use.  

### Step 2: Setting Up the YouTube Data API  

Now you are ready to create a pipeline to process YouTube video details. As part of this process, you need to set up the **YouTube Data API v3**. Follow the steps below:  

#### 1. Access Google Cloud Platform (GCP)  
1. Log in with your Google account and go to the [Google Cloud Platform (GCP)](https://console.cloud.google.com/).  
2. Create a new project in GCP.  

#### 2. Enable YouTube Data API v3  
1. In your project, navigate to the **APIs & Services** menu.  
2. Click **+ Enable APIs and Services** to access the APIs Library.  
3. Search for **YouTube Data API v3** in the library and enable it.  

#### 3. Create API Key  
1. Return to the **APIs & Services** menu and select **Credentials** from the left-hand menu.  
2. Click on **Create Credentials** and select **API Key**.  
3. Ensure you select the **YouTube Data API v3** and choose **Public Data** as the access level.  

#### 4. Save Your API Key  
You will use the generated API key in your Databricks pipeline script to authenticate requests to the YouTube Data API. Keep it secure and accessible for later use.  

## Step 3: Writing the Data Pipeline Script  

To build the ETL pipeline, you will write a Python script. Before starting, ensure the required library is installed.  

### 1. Install `google-api-python-client`  
Run the following command to install the library:  
```bash
pip install google-api-python-client
```
> **Note:** After installing, comment out this line in your script to avoid reinstalling it every time. You will need this library later when creating and running your job.

### 2: Python Data Pipeline Script
You need to create a notebook from your workspace to write your script for the ETL pipeline. 

### Random Country Selection Function Added To Final ETL Pipeline Script  
Later in the process, I decided to enhance the pipeline by using a random country name as the keyword for fetching YouTube video details. 
To achieve this, I created a separate notebook named **`countries`** that contains a dictionary of country names and their corresponding country codes, which I sourced from the internet.  

In the pipeline notebook, I imported the `countries` notebook using the following magic command:  
```python
%run ./countries
```
Here’s the code snippet used to select a random country:
```python
import random

random_country = random.choice(countries)
keyword = random_country[1]
print(keyword)

maxResults = 50
file_name = f"search_{keyword}_video_results"
```

Below is the complete Python script for the ETL pipeline:
```python
from googleapiclient.discovery import build
import json
import pyspark 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, explode
from pyspark.sql.functions import lit

def extract_video_details_by_keyword(keyword, maxResults):
    youtube = build('youtube', 'v3', developerKey=<your google api key>)
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

```

After setting up the script and making the necessary changes, you can execute the functions to run the ETL pipeline:
```python
details = extract_video_details_by_keyword(keyword, maxResults)
file_path = loadRawData(details, file_name)
result_df = transformData(keyword, file_path)
loadIntoDeltaTable(result_df)
```
### Description of the data pipeline
> **Note:** I wrote description of each function in docstring which you can find in [my github repository](https://github.com/LeeJihyun99/End-to-End-Data-Engineering-Project-with-Databricks).

## Functions

### 1. `extract_video_details_by_keyword(keyword, maxResults)`
This function fetches video details from the YouTube Data API v3 using a given keyword (such as a country name) and retrieves up to a specified number of video results.

### 2. `loadRawData(data, file_name)`
This function saves the raw YouTube API response data into a JSON file in Databricks FileStore and returns the file path for further processing.

### 3. `transformData(keyword, file_path)`
This function reads the raw JSON data from the given file, flattens the nested structure, adds the keyword as a column, and returns a structured Spark DataFrame with the relevant video details.

### 4. `loadIntoDeltaTable(result_df)`
This function loads the transformed DataFrame into a Delta table, ensuring that only unique records (based on `videoId`) are added to the table.

