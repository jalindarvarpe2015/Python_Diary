import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("myspark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


# Explode Function in PySpark

#'''It is a PySpark function that return  new row for each element in the given array or map
#array - List of Element
#map - list of key and value pair

#uses the default column name col for element in the array and key and value for element in the map '''

#''' Explode have 4 varients

#1) explode
#when an array is passed  to this function it creates new row for each element in array.
#when map is passed it creates two new column one for key and second for value and each element in map split into the rows.
#if the array or map is null that row is eliminated.

#2)explode_outer
#unlike explode, if the array or map is null explode outer return null'

#3)posexlode
#when array or map is passed it creates positional columnn also for each element ignore the null element 

#4)posexplode
#unlike posexplode, if the array or map is null explode_outer return null



array_appliance = [
        ('Raja', ['TV', 'Refrigerator', 'Oven', 'AC']),
        ('Raghav', ['AC', 'Washing Machine', None]),
        ('Ram', ['Grinder', 'TV']),
        ('Ramesh', ['Refrigerator', 'TV', None]),
        ('Rajesh', None)]

df_app = spark.createDataFrame(data=array_appliance, schema=['Name','Applience'])
df_app.show()



