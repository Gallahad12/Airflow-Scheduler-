from pymongo import MongoClient
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

client = MongoClient("mongodb+srv://Gallahad:Lancelot_12@project.pkiweil.mongodb.net/")

path = "/opt/airflow/data/Supplement_Sales_Weekly_Expanded_Clean.csv"
Data_Clean = pd.read_csv(path)

Data_Document = Data_Clean.to_dict(orient='records')

client['Milestone3']['Sales_Performance'].insert_many(Data_Document)