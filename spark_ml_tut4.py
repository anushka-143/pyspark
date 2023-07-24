import pyspark
import pandas as pd

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ML_tut').getOrCreate()

training = spark.read.csv('test2.csv',header=True,inferSchema=True)

from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=['Exp'],outputCol='Independent Feature')
output = assembler.transform(training)

final_data = output.select('Independent Feature','Salary')
final_data.show()

from pyspark.ml.regression import LinearRegression
train_data,test_data = final_data.randomSplit([0.7,0.3])
regressor =LinearRegression(featuresCol='Independent Feature',labelCol='Salary')
regressor = regressor.fit(train_data)

pred = regressor.evaluate(test_data)
pred.predictions.show()

#checking error
print(pred.meanAbsoluteError,pred.meanSquaredError)