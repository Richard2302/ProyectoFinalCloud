import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
from pyspark.sql.functions import substring, length, col, expr
from pyspark.sql.types import IntegerType

conf = SparkConf().setAppName('averagePrice')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "Airbnb barcelona.csv"

df = spark.read.option('header', 'true').csv(path)

df = df.select(df['zipcode'], df['price'], f.regexp_replace(f.col('price'), '[\$]', '').alias('priceAux'), df['review_scores_rating'])
df = df.withColumn('priceAux', expr("substring(priceAux, 1, length(priceAux)-4)"))
df.show()
df = df.groupBy(df['zipcode']).agg(f.avg(df['priceAux']).alias('averagePrice')).sort('zipcode')

df.show()
