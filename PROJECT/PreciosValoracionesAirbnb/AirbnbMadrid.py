import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
from pyspark.sql.functions import substring, length, col, expr
import pandas as pd

conf = SparkConf().setAppName('AirbnbMadrid')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "Airbnb_madrid.csv"

df = spark.read.option('header', 'true').csv(path)

#PRECIO MEDIO POR BARRIO (ASCENDENTE)
dfPrice = df.select(df['neighbourhood_cleansed'], df['price'])
dfPrice = dfPrice.na.drop()
dfPrice = dfPrice.withColumnRenamed('neighbourhood_cleansed', 'neighbourhood')
dfPrice = dfPrice.withColumn('price', f.regexp_replace(f.col('price'), '[\$]', '').alias('priceAux'))
dfPrice = dfPrice.withColumn('priceAux', expr("substring(price, 0, length(price)-4)"))
dfPrice = dfPrice.groupBy(dfPrice['neighbourhood']).agg(f.avg(dfPrice['priceAux']).alias('averagePrice'))
dfPrice = dfPrice.sort(dfPrice['averagePrice'])
#dfPrice.show(80)

#EXCEL
dfPrice.toPandas().to_excel("averagePriceMadridNeighbourhoods.xlsx", index=False)

topPriceMadridNeighbourhoods = dfPrice.toPandas().iloc[:10, :]
topPriceMadridNeighbourhoods.to_excel("topPriceMadridNeighbourhoods.xlsx", index=False)

#PUNTUACION MEDIA POR BARRIO (DESCENDENTE)
dfRating = df.select(df['neighbourhood_cleansed'], df['review_scores_rating'])
dfRating = dfRating.na.drop()
dfRating = dfRating.withColumnRenamed('neighbourhood_cleansed', 'neighbourhood')
dfRating = dfRating.groupBy(dfRating['neighbourhood']).agg(f.avg(dfRating['review_scores_rating']).alias('averageRating'))
dfRating = dfRating.sort(f.desc(dfRating['averageRating']))
#dfRating.show(80)

#EXCEL
dfRating.toPandas().to_excel("averageRatingMadridNeighbourhoods.xlsx", index=False)

topRatingMadridNeighbourhoods = dfRating.toPandas().iloc[:10, :]
topRatingMadridNeighbourhoods.to_excel("topRatingMadridNeighbourhoods.xlsx", index=False)

print('---------------SUCCESS---------------')
