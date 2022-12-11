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

dfPrice = df.select(df['zipcode'], df['price'])
#dfPrice = dfPrice.withColumn('zipcode', f.regexp_extract_all(f.col('zipcode'), '8[0-9]{3}'))

dfPrice = dfPrice.withColumn('price', f.regexp_replace(f.col('price'), '[\$]', '').alias('priceAux'))
dfPrice = dfPrice.withColumn('priceAux', expr("substring(price, 0, length(price)-4)"))
dfPrice = dfPrice.groupBy(dfPrice['zipcode']).agg(f.avg(dfPrice['priceAux']).alias('averagePrice'))
dfPrice.show(80)

dfRating = df.select(df['zipcode'], df['review_scores_rating'])
dfRating = dfRating.groupBy(df['zipcode']).agg(f.avg(dfRating['review_scores_rating']).alias('averageRating'))
dfRating.show(80)
