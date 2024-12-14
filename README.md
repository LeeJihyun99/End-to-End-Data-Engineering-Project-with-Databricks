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
1. Return to the **APIs & Services** menu and select **Login Details(Credentials)** from the left-hand menu.  
2. Click on **Create Credentials** and select **API Key**.
4. Ensure you select the **YouTube Data API v3** and choose **Public Data** as the access level.  
![image](https://github.com/user-attachments/assets/fb877b4e-8488-4f90-818d-8d7532016e21)

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

### Create Delta Tables with Schemas
Before executing the ETL script, you need to create the necessary Delta tables with predefined schemas to ensure data can be appended correctly. Here's how to create the tables:

1. **Create the Delta Database** (if it doesn't already exist):
   ```sql
   %sql
   CREATE DATABASE IF NOT EXISTS delta_tables;
   ```
2. You need to **create a table with the appropriate schema*** where your data will be stored. Use the following SQL command to create the youtube_video_results_by_keyword table in the delta_tables database:
  ```sql
   %sql
CREATE TABLE IF NOT EXISTS delta_tables.youtube_video_results_by_keyword (
  keyword STRING,
  etag STRING,
  nextPageToken STRING,
  videoId STRING,
  title STRING,
  description STRING,
  channelTitle STRING, 
  publishedAt TIMESTAMP
);
  ```
### Execute the script
After setting up the script and making the necessary changes, you can execute the functions to run the ETL pipeline:
```python
details = extract_video_details_by_keyword(keyword, maxResults)
file_path = loadRawData(details, file_name)
result_df = transformData(keyword, file_path)
loadIntoDeltaTable(result_df)
```
### Description of the data pipeline
> **Note:** I wrote description of each function in docstring which you can find the scripts from this repository.

## Functions

### 1. `extract_video_details_by_keyword(keyword, maxResults)`
This function fetches video details from the YouTube Data API v3 using a given keyword (such as a country name) and retrieves up to a specified number of video results.

### 2. `loadRawData(data, file_name)`
This function saves the raw YouTube API response data into a JSON file in Databricks FileStore(DBFS) and returns the file path for further processing.
#### What is DBFS?

DBFS (Databricks File System) is a distributed file system mounted into a Databricks workspace. It allows you to store and access files in a unified environment, making it easy to work with data. DBFS can be accessed from notebooks, jobs, and even via the REST API. It provides a convenient location for storing data files, logs, and models within the Databricks platform.

> **Note:** Enabling DBFS in Databricks: To use DBFS, you need to enable it in the **Advanced settings** of your Databricks workspace. Ensure that DBFS is enabled before trying to save or access files within the Databricks environment.
> ![image](https://github.com/user-attachments/assets/7e870044-d074-4c91-9466-4599ea1cd411)


#### ! How to Download JSON Files from DBFS to Local Machine ! 

If your file's path in DBFS is:
```
dbfs:/FileStore/tables/<filename>
```

To download the file to your local machine, follow these steps:

1. **Get the URL from your Azure Databricks workspace instance**:
   - Go to the **Azure portal** and find your **Databricks workspace instance**.

2. **Construct the download URL**:
   - Add the path `/files/tables/` followed by your file's name and format.
   - Example URL structure:
   ```
   https://<databricks-instance-id>.azuredatabricks.net/files/tables/<filename.format>/
   ```
  Simply open the constructed URL in your browser. The file will be automatically downloaded to your local machine.
  
3. **View JSON data**:
- Once you download the JSON file, you can use tools such as [JSON Viewer](https://jsonviewer.tools/editor) to view deeply nested or complicated JSON data in a tree format for easier exploration.
![image](https://github.com/user-attachments/assets/dfa6f800-2b4f-48f0-90c9-209745bb9326)

### 3. `transformData(keyword, file_path)`
This function reads the raw JSON data from the given file, flattens the nested structure, adds the keyword as a column, and returns a structured Spark DataFrame with the relevant video details.

### 4. `loadIntoDeltaTable(result_df)`
This function loads the transformed DataFrame into a Delta table, ensuring that only unique records (based on `videoId`) are appended to the table.

In **Databricks**, when you write data to a Delta table, it is automatically stored in **Delta Lake**—an open-source storage layer built on top of Apache Spark. Delta Lake offers several key benefits:

- **ACID transactions**: Ensures data consistency and reliability, even in the event of failures.
- **Schema enforcement**: Automatically enforces the expected data structure, preventing corrupted or invalid data from being written.
- **Efficient querying**: Delta tables are optimized for faster reads and writes, improving performance and reducing resource consumption.

By storing data in Delta tables within Delta Lake, you benefit from features such as:
- **Time travel**: Enables querying of historical versions of the data.
- **Scalability**: Delta Lake efficiently handles large datasets with built-in optimizations for processing.
- **Incremental data loading**: Supports incremental updates, allowing for more efficient data processing without the need to reload the entire dataset.

Storing the data in Delta tables within Delta Lake ensures better data management, enhanced processing efficiency, and the ability to track and manage data changes over time, making it a superior choice for big data workloads in Databricks.

### Using Delta Tables for Queries

In order to make queries for the Delta tables, especially for data analysts or scientists, you can create a **Data Warehouse** in Databricks. A data warehouse provides an optimized environment for running SQL queries on the data stored in Delta tables.

Once the warehouse is created, you can write and execute SQL queries directly within the Databricks environment to analyze and explore the data stored in the Delta tables.
![image](https://github.com/user-attachments/assets/26f0afb7-ade5-49dc-b315-24998e54eac9)

Data analysts do not need to execute notebooks to analyze the data. Instead, they can simply write SQL queries to query the Delta tables directly in Databricks.
![image](https://github.com/user-attachments/assets/18768087-caec-4860-9338-0f59b1f71821)


## Step 4: Create a Job Schedule for Automation

Now that your data pipeline is ready, you can create a **job schedule** to automate the data pipeline workflow. This will ensure that your pipeline runs at specified intervals without manual intervention.

### Important Note:
Before scheduling the job, you need to **uncomment** the line `pip install google-api-python-client` in your notebook. If you don't uncomment it, the job will fail to proceed since the required `googleapiclient` library won't be installed when the job runs.

For detailed instructions on setting up job schedules, you can refer to the article that inspired this project. The process will involve setting up the job in Databricks and specifying the necessary parameters (e.g., cluster configuration, schedule, etc.) for automation.

Once the job is scheduled, it will automatically run according to the set frequency, ensuring the pipeline operates continuously without requiring manual execution.

## Step 5: Create a Git Folder for Commit and Push in Databricks

To manage your project code effectively, you can create a **Git folder** in Databricks. This allows you to commit and push your changes directly from the Databricks environment, integrating it with your version control system (e.g., GitHub, GitLab, Bitbucket).

### How to Set Up Git Integration:

1. **Create a New GitHub Repository**:
   - First, go to your GitHub account and create a new repository where you want to store your project.
   
2. **Create a Personal Access Token**:
   - In GitHub, go to **Developer Settings** > **Personal Access Tokens**.
   - Create a new token with the required scopes for your project (e.g., repo, workflow).
   - **Important**: Save this token somewhere securely because it will not be visible again unless you generate a new one.

3. **Link GitHub to Databricks**:
   - In Databricks, navigate to the **User Settings**.
   - Under the **Linked Accounts** section, select **GitHub** and enter the personal access token you just created.

4. **Create a Git Folder in Databricks**:
   - In the Databricks workspace, right-click on your workspace and select **Create** > **Git Folder**.
   - This folder will now be linked to your GitHub repository.

5. **Commit and Push Changes**:
   - Once you make changes to your notebook or script, you can commit those changes directly to your GitHub repository.
   - Use the **Git** tab in Databricks to commit your changes and push them to the repository.

This integration ensures version control, easier collaboration, and streamlined project management, especially for larger teams or when managing multiple versions of your project.

## Conclusion

Thank you for reading through my project! 😊 If you have any questions, suggestions, or ideas for improvements, feel free to contact me. I'm always open to feedback and happy to help!

Good luck with your project, and I hope this guide was helpful! 👍

