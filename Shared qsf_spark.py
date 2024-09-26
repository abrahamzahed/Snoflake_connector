# Databricks notebook source
import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 1000)

#%pip install plotly
#%pip install tabulate
import plotly.express as px
import warnings
#warnings.filterwarnings('ignore')
import seaborn as sns
import matplotlib.pyplot as plt

import plotly.graph_objects as go
import plotly.io as pio

from pyspark.sql.functions import *
from pyspark.sql.window import *

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import requests
import json
from pyspark.sql import SparkSession
import base64
from urllib.parse import urlencode
#from tabulate import tabulate
from pyspark.sql import *

#import statsmodels.api as sm
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.window import Window
spark = SparkSession.builder.getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, round
from pyspark.sql.window import Window
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pyspark.ml.feature import StringIndexer
from pyspark.ml.stat import ChiSquareTest
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml import Pipeline
from pyspark.sql.utils import IllegalArgumentException
from pyspark.sql.functions import desc

import sys
import time


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def qsf_spark(query: str, table_name: str):
#def qsf_spark(query: str, table_name: str, col_name_list: list):
  #spark = SparkSession.builder.getOrCreate()

  client_id = 'D5Egd8/F2xdQUM23ivYyxx5GZ40='
  client_secret = 'yC7Bd8/Mg2GaETBzICzMq+3OVJCgJYojtSJ41sSp/30='
  redirect_uri = 'https://localhost.com'
  authorization_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/authorize'
  token_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/token-request'
  refresh_token = 'xxxxxxxxxxxxxxxxxxxxxxxxxx'
    
  # Generate Access Token
  hdrs = {'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(client_id, client_secret).encode()).decode()),'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'}
  data1 = urlencode({'grant_type': 'refresh_token','refresh_token': refresh_token,'redirect_uri': redirect_uri})
  data = data1.encode('ascii')
  r = requests.post(token_endpoint, headers=hdrs, data=data)
  TOKEN = r.json()['access_token']

  # Set Snowflake options below.
  sfOptions = {
  "sfURL" : "tmobile.west-us-2.privatelink.snowflakecomputing.com",
  "sfUser" : "xxxxxxxxxxxxxxxxxxxxxxxxxx@T-MOBILE.COM",
  "sfAuthenticator" : "oauth",
  "sfToken" : None,
  "sfDatabase" : "BDM_CDB_DB",
  "sfSchema" : "CDB_A",
  "sfWarehouse" : "xxxxxxxxxxxxxxxxxxxxxxxxxx"
  }

  # This line updates the sfToken value in the Snowflake options above. 
  sfOptions["sfToken"] = TOKEN
  
  SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  # Connect to Snowflake and build data frame.
  df = (spark.read.format(SNOWFLAKE_SOURCE_NAME)
  .options(**sfOptions)
  .option("query", query)
  .load())

  #Write data into delta table (overwirte if it exists)
  #spark.sql("CREATE SCHEMA IF NOT EXISTS CARE_BI")
  spark.sql("USE CARE_BI")
  
  # Drop the table if it already exists
  spark.sql(f"DROP TABLE IF EXISTS {table_name}")
  df.write.format("delta").saveAsTable(table_name)
  
  # Set the properties for auto optimize and auto compact
  spark.sql("SET spark.databricks.delta.properties.autoOptimize.optimizeWrite = true")
  spark.sql("SET spark.databricks.delta.properties.autoOptimize.autoCompact = true")

  # Create a comma-separated string of column names for Z-Ordering
  #cols = ', '.join(col_name_list)

  # Optimize and Z-Order the table by given columns
  #spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")

  # Read the table from the "collection_strategy" schema
  df_delta = spark.read.format("delta").table(table_name)
  #

  return df_delta

# COMMAND ----------

import base64
from urllib.parse import urlencode
import requests
from pyspark.sql import SparkSession

def wsf_spark(df, schema, table_name):
    # Initialize Spark session
    #spark = SparkSession.builder.getOrCreate()

    client_id = 'D5Egd8/F2xdQUM23ivYyxx5GZ40='
    client_secret = 'yC7Bd8/Mg2GaETBzICzMq+3OVJCgJYojtSJ41sSp/30='
    redirect_uri = 'https://localhost.com'
    authorization_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/authorize'
    token_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/token-request'
    refresh_token = 'xxxxxxxxxxxxxxxxxxxxxxxxxx'

    # Generate Access Token
    hdrs = {'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(client_id, client_secret).encode()).decode()), 'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'}
    data = urlencode({'grant_type': 'refresh_token', 'refresh_token': refresh_token, 'redirect_uri': redirect_uri})
    data = data.encode('ascii')
    r = requests.post(token_endpoint, headers=hdrs, data=data)
    access_token = r.json()['access_token']

    # Write the DataFrame to the Snowflake table
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(
            sfURL='tmobile.west-us-2.privatelink.snowflakecomputing.com',
            sfUser='xxxxxxxxxxxxxxxxxxxxxxxxxx@T-MOBILE.COM',
            sfAuthenticator='oauth',
            sfDatabase='BDM_CDB_DB',
            sfSchema=schema,
            sfWarehouse='xxxxxxxxxxxxxxxxxxxxxxxxxx',
            sfToken=access_token
        ) \
        .option("dbtable", table_name) \
        .mode("append") \
        .save()

# Example usage
# Assuming you have a Spark DataFrame df and want to upload it to Snowflake in schema "CDB_A" with table name "example_table"
#df = spark.sql("select 1+1")
#wsf_spark(df, "CDB_A", "example_table")


# COMMAND ----------

import base64
from urllib.parse import urlencode
import requests
from pyspark.sql import SparkSession

client_id = 'D5Egd8/F2xdQUM23ivYyxx5GZ40='
client_secret = 'yC7Bd8/Mg2GaETBzICzMq+3OVJCgJYojtSJ41sSp/30='
redirect_uri = 'https://localhost.com'
authorization_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/authorize'
token_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/token-request'
refresh_token = 'ver:2-hint:34965336494990-did:2006-ETMsDgAAAY3CZ/1EABRBRVMvQ0JDL1BLQ1M1UGFkZGluZwEAABAAEKWerxSPLn6awzBM0yfEYbsAAAEAjjDwxMjriV3LsDpFKSpEe8yB2HuHfw8x32BphbuNuuyYgEue1e32ScDO4pb5WGAKhCEMvBxUCCL0esUO8Ij8NGrCNtlkrpvQD6KpAHUHJiRXZTiwpjfzEEoWz+v/SSTRrDVYbcrbxo3IqVClnBfP0V4POkcPJenpKdilw24a69POJKP5tJj4h3I5Pc9dJJp7pDbzcUS8QvLztc4FtjSijeJXhUTpHNc4FRGpS1X48PDbLTH/VCvk/Fen1S36p4jg+m+7ki32QzJho4N8ID5FeoCF0+pqlQpqm9v268AwpLrZM7qZDoLidD2aYc5cmvPM1BdE+5PpSSJicRPxbNnNmAAUp5sl7bqJzFt2n3/N2wcV9J00zjc='

def get_access_token(client_id, client_secret, refresh_token, token_endpoint):
    hdrs = {'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(client_id, client_secret).encode()).decode()), 'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'}
    data = urlencode({'grant_type': 'refresh_token', 'refresh_token': refresh_token})
    data = data.encode('ascii')
    r = requests.post(token_endpoint, headers=hdrs, data=data)
    return r.json().get('access_token')

def create_table_from_df_spark(df, schema, table_name):
#def create_table_from_df_spark(df):
    #schema = "CDB_A"
    #table_name = "AZ_example_table"
    # Initialize Spark session
    #spark = SparkSession.builder.getOrCreate()

    # Refresh the access token
    access_token = get_access_token(client_id, client_secret, refresh_token, token_endpoint)

    # Write the DataFrame to the Snowflake table
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(
            sfURL='tmobile.west-us-2.privatelink.snowflakecomputing.com',
            sfUser='SHANGYU.NI47@T-MOBILE.COM',
            sfAuthenticator='oauth',
            sfDatabase='BDM_CDB_DB',
            sfSchema=schema,
            sfWarehouse='BDM_NBA_BI_PRD_WH_02',
            sfToken=access_token
        ) \
        .option("dbtable", table_name) \
        .mode("append") \
        .option("batchsize", 100000) \
        .save()

# Example usage
# Assuming you have a Spark DataFrame df and want to upload it to Snowflake in schema "CDB_A" with table name "example_table"
# Set your client_id, client_secret, refresh_token, and token_endpoint
 create_table_from_df_spark(df, "CDB_A", "example_table")


# COMMAND ----------

df = spark.sql("select * from care_bi.sni_nba_line_volchurn_model_030124")
sf_table_name = "sni_nba_line_volchurn_model_030124"
create_table_from_df_spark(df, "CDB_A", sf_table_name)

# COMMAND ----------

#mode("append") : Will append the data if the table exists. if not it will create a new table.

#mode("overwrite"): This mode replaces the existing table or data in the table if it already exists. It essentially drops the existing table and creates a new one with the data from the DataFrame.

#mode("ignore"): If the table already exists, the "ignore" mode skips writing the DataFrame and does not make any changes to the existing table. It's useful when you want to avoid accidentally overwriting data.

#mode("error"): This mode throws an error if the table already exists. It prevents accidental overwrites and requires manual intervention to resolve the conflict.

#mode("errorifexists"): Similar to "error" mode, "errorifexists" throws an error if the table already exists. It's a more explicit way of specifying that an error should occur if the table is found.

# COMMAND ----------

def process_to_sf(df, n, db_delta_table, sf_schema, sf_table_name):
    def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█', print_end="\r"):
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        percent = float(percent) if float(percent) <= 100.0 else 100.0  # Ensure percent does not exceed 100
        filled_length = int(length * iteration // total)
        bar = fill * filled_length + '-' * (length - filled_length)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
        if iteration == total:
            print()

    df_list = []
    chunk_size = df.count() // n
    processed_rows = 0  

    for i in range(n+1):
        lower_bound = i * chunk_size + 1
        upper_bound = (i + 1) * chunk_size
        query = f"SELECT * FROM {db_delta_table} WHERE row_number >= {lower_bound} AND row_number <= {upper_bound}"
        df_chunk = spark.sql(query)
        create_table_from_df_spark(df_chunk, sf_schema, sf_table_name)
        processed_rows += df_chunk.count()  
        if processed_rows > df.count():  
            processed_rows = df.count()  
        print_progress_bar(processed_rows, df.count(), prefix='Processing:', suffix='Complete', length=50)

# COMMAND ----------

##example to use above functions
df = spark.sql("select * from care_bi.sni_nba_line_volchurn_model_020124")
n = 6
db_delta_table = "care_bi.sni_nba_line_volchurn_model_020124"
sf_schema = "CDB_A"
sf_table_name = "sni_nba_line_volchurn_model_020124"
process_to_sf(df, n, db_delta_table, sf_schema, sf_table_name)

# COMMAND ----------

#####%sql
#####CREATE TABLE if not exists care_bi.az_base_sms_call_temp AS
#####SELECT * EXCEPT (row_number), ROW_NUMBER() OVER (ORDER BY ban) AS row_number
#####FROM care_bi.az_base_sms_call;
#####--Step 2: Drop the old table
#####DROP TABLE care_bi.az_base_sms_call;
#####-- Step 3: Rename the new table to match the original table's name
#####ALTER TABLE care_bi.az_base_sms_call_temp RENAME TO care_bi.az_base_sms_call;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from care_bi.az_base_sms_call where TEMPLATE_ID in ('T-6vUqeJigClM') and is_matched = 1 
# MAGIC and SMS_MSISDN  = 19107971173 limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC T-qcUfHVNeL7T
# MAGIC T-y3PRGarNSQ1
# MAGIC T-Pxihhb6jWnI
