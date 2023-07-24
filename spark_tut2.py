'''Pyspark dataframes'''

import pandas as pd
import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('practice').getOrCreate()

#read the dataset
df_spark = spark.read.option('header','true').csv('test.csv',inferSchema=True)
#print(df_spark.printSchema())

#print(type(df_spark))
#print(df_spark.columns)
#df_spark.select(['Name','Age']).show()
#df_spark.describe().show()
'''#add column
df_spark=df_spark.withColumn('Age after two years',df_spark['Age']+2)

#drop column
df_spark = df_spark.drop('Age after two years')

#rename
df_spark=df_spark.withColumnRenamed('Name','New Name')
'''

#drop rows with Nan value
#df_spark.na.drop().show()

#how -> how = 'any' - drop row if there is any nan value, how = 'all'  - drop when all the values in row is nan

#when we want to drop nan values from only one column
'''df_spark.na.drop(how = 'any', subset = ['Experience])'''

#filling missing values
#df_spark.na.fill('Missing Values').show()
from pyspark.ml.feature import Imputer
imputer = Imputer(inputCols=['Age','Experience'],
                  outputCols=['{}_imputed'.format(c) for c in ['Age','Experience']]).setStrategy('mean')
imputer.fit(df_spark).transform(df_spark).show()
df_spark = df_spark.drop('Age','Experience')


'''Filter operation, &,|,==,~'''

#filter
#df_spark.filter("Age_imputed>=22").show()
#df_spark.filter(df_spark['Experience_imputed']>=22).show()
