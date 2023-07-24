import pandas as pd
import pyspark

df = pd.read_excel('test2.xlsx')
df = df.to_csv('test2.csv')

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Agg').getOrCreate()

'''practing Groupby and aggregate'''
df_spark = spark.read.csv('test2.csv',header=True,inferSchema=True)


#Groupby
df_spark.groupBy('Name').sum().show()

#Aggregate
df_spark.agg({'Salary':'sum'}).show()

#checking the max
df_spark.groupBy('Name').max().show()




