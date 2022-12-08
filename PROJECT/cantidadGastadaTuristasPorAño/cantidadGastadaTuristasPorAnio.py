import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
import pandas as pd
import matplotlib as plt


conf = SparkConf().setAppName('cantidadGastadaPorAnio')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "llegadasTuristasVisitantesYExpenditure.csv"

df = spark.read.option("header", "true").csv(path)
df.show()
#df1 = df.select(df["Tourist/visitor arrivals and tourism expenditure"])
#df1 = df.select(df["_c2"])
#df2 = df.select(df["_c3"])
#df3 = df.select(df["_c4"])
#df4 = df.select(df["_c5"])

#df1.show()
#df1.show()
#df1.show()
#df1.show()
df.printSchema()
