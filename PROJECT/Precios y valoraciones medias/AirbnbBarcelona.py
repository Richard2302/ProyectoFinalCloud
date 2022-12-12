import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
from pyspark.sql.functions import substring, length, col, expr
import pandas as pd

conf = SparkConf().setAppName('AirbnbBarcelona')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "Airbnb_barcelona.csv"

df = spark.read.option('header', 'true').csv(path)

#PRECIO MEDIO POR CODIGO POSTAL (ASCENDENTE)
dfPrice = df.select(df['zipcode'], df['price'])
dfPrice = dfPrice.na.drop()
dfPrice = dfPrice.withColumn('zipcode', f.regexp_extract(f.col('zipcode'), '8[0-9]{3}', 0))
#dfPrice = dfPrice.filter(dfPrice['price'].rlike('8[0-9]{3}'))
dfPrice = dfPrice.withColumn('price', f.regexp_replace(f.col('price'), '[\$]', '').alias('priceAux'))
dfPrice = dfPrice.withColumn('priceAux', expr("substring(price, 0, length(price)-4)"))
dfPrice = dfPrice.groupBy(dfPrice['zipcode']).agg(f.avg(dfPrice['priceAux']).alias('averagePrice'))
dfPrice.sort(dfPrice['averagePrice']).show()

#EXCEL
dfPrice.toPandas().to_excel("averagePriceBarcelonaZipcode.csv", index=False)

#PUNTUACION MEDIA POR CODIGO POSTAL (DESCENDENTE)
dfRating = df.select(df['zipcode'], df['review_scores_rating'])
dfRating = dfRating.na.drop()
dfRating = dfRating.withColumn('zipcode', f.regexp_extract(f.col('zipcode'), '8[0-9]{3}', 0))
dfRating = dfRating.groupBy(dfRating['zipcode']).agg(f.avg(dfRating['review_scores_rating']).alias('averageRating'))
dfRating.sort(f.desc(dfRating['averageRating'])).show()

#EXCEL
dfRating.toPandas().to_excel("averageRatingBarcelonaZipcode.csv", index=False)

#PRECIO MEDIO POR BARRIO (ASCENDENTE)
dfPrice2 = df.select(df['neighbourhood_cleansed'], df['price'])
dfPrice2 = dfPrice2.na.drop()
dfPrice2 = dfPrice2.withColumn('neighbourhood_cleansed', f.regexp_extract(f.col('neighbourhood_cleansed'), '^([^0-9]*)$', 0))
dfPrice2 = dfPrice2.withColumn('price', f.regexp_replace(f.col('price'), '[\$]', '').alias('priceAux'))
dfPrice2 = dfPrice2.withColumn('priceAux', expr("substring(price, 0, length(price)-4)"))
dfPrice2 = dfPrice2.groupBy(dfPrice2['neighbourhood_cleansed']).agg(f.avg(dfPrice2['priceAux']).alias('averagePrice'))
dfPrice2 = dfPrice2.na.drop()
dfPrice2.sort(dfPrice2['averagePrice']).show(80)

#EXCEL
dfPrice2.toPandas().to_excel("averagePriceBarcelonaNeighbourhoods.csv", index=False)

#PUNTUACION MEDIA POR BARRIO (DESCENDENTE)
dfRating2 = df.select(df['neighbourhood_cleansed'], df['review_scores_rating'])
dfRating2 = dfRating2.na.drop()
dfRating2 = dfRating2.withColumn('neighbourhood_cleansed', f.regexp_extract(f.col('neighbourhood_cleansed'), '^([^0-9]*)$', 0))
dfRating2 = dfRating2.groupBy(dfRating2['neighbourhood_cleansed']).agg(f.avg(dfRating2['review_scores_rating']).alias('averageRating'))
dfRating2 = dfRating2.na.drop()
dfRating2.sort(f.desc(dfRating2['averageRating'])).show(80)

#EXCEL
dfRating2.toPandas().to_excel("averageRatingBarcelonaNeighbourhoods.csv", index=False)
