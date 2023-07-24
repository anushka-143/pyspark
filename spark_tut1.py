import pyspark
import pandas as pd

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()

df = pd.read_excel('test.xlsx')
df.to_csv('test.csv')
#print(df)

df_spark = spark.read.csv('test.csv')
#df_spark.show()
df_pyspark = spark.read.option('header','true').csv('test.csv').show()
