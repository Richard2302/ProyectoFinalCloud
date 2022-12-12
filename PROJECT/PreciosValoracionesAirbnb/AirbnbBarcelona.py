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
dfPrice = dfPrice.withColumn('price', f.regexp_replace(f.col('price'), '[\$]', '').alias('priceAux'))
dfPrice = dfPrice.withColumn('priceAux', expr("substring(price, 0, length(price)-4)"))
dfPrice = dfPrice.groupBy(dfPrice['zipcode']).agg(f.avg(dfPrice['priceAux']).alias('averagePrice'))
dfPrice = dfPrice.sort(dfPrice['averagePrice'])
#dfPrice.show()

#EXCEL
dfPrice.toPandas().to_excel("averagePriceBarcelonaZipcode.xlsx", index=False)

topPriceBarcelonaZipcode = dfPrice.toPandas().iloc[:10, :]
topPriceBarcelonaZipcode.to_excel("topPriceBarcelonaZipcode.xlsx", index=False)

#PUNTUACION MEDIA POR CODIGO POSTAL (DESCENDENTE)
dfRating = df.select(df['zipcode'], df['review_scores_rating'])
dfRating = dfRating.na.drop()
dfRating = dfRating.withColumn('zipcode', f.regexp_extract(f.col('zipcode'), '8[0-9]{3}', 0))
dfRating = dfRating.groupBy(dfRating['zipcode']).agg(f.avg(dfRating['review_scores_rating']).alias('averageRating'))
dfRating = dfRating.sort(f.desc(dfRating['averageRating']))
#dfRating.show()

#EXCEL
dfRating.toPandas().to_excel("averageRatingBarcelonaZipcode.xlsx", index=False)

topRatingBarcelonaZipcode = dfRating.toPandas().iloc[:10, :]
topRatingBarcelonaZipcode.to_excel("topRatingBarcelonaZipcode.xlsx", index=False)

#PRECIO MEDIO POR BARRIO (ASCENDENTE)
dfPrice2 = df.select(df['neighbourhood_cleansed'], df['price'])
dfPrice2 = dfPrice2.na.drop()
dfPrice2 = dfPrice2.withColumnRenamed('neighbourhood_cleansed', 'neighbourhood')
dfPrice2 = dfPrice2.withColumn('neighbourhood', f.regexp_extract(f.col('neighbourhood'), '^([^0-9]*)$', 0))
dfPrice2 = dfPrice2.withColumn('price', f.regexp_replace(f.col('price'), '[\$]', '').alias('priceAux'))
dfPrice2 = dfPrice2.withColumn('priceAux', expr("substring(price, 0, length(price)-4)"))
dfPrice2 = dfPrice2.groupBy(dfPrice2['neighbourhood']).agg(f.avg(dfPrice2['priceAux']).alias('averagePrice'))
dfPrice2 = dfPrice2.na.drop()
dfPrice2 = dfPrice2.sort(dfPrice2['averagePrice'])
#dfPrice2.show()

#EXCEL
dfPrice2.toPandas().to_excel("averagePriceBarcelonaNeighbourhoods.xlsx", index=False)

topPriceBarcelonaNeighbourhoods = dfPrice2.toPandas().iloc[:10, :]
topPriceBarcelonaNeighbourhoods.to_excel("topPriceBarcelonaNeighbourhoods.xlsx", index=False)

#PUNTUACION MEDIA POR BARRIO (DESCENDENTE)
dfRating2 = df.select(df['neighbourhood_cleansed'], df['review_scores_rating'])
dfRating2 = dfRating2.na.drop()
dfRating2 = dfRating2.withColumnRenamed('neighbourhood_cleansed', 'neighbourhood')
dfRating2 = dfRating2.withColumn('neighbourhood', f.regexp_extract(f.col('neighbourhood'), '^([^0-9]*)$', 0))
dfRating2 = dfRating2.groupBy(dfRating2['neighbourhood']).agg(f.avg(dfRating2['review_scores_rating']).alias('averageRating'))
dfRating2 = dfRating2.na.drop()
dfRating2 = dfRating2.sort(f.desc(dfRating2['averageRating']))
#dfRating2.show()

#EXCEL
dfRating2.toPandas().to_excel("averageRatingBarcelonaNeighbourhoods.xlsx", index=False)

topRatingBarcelonaNeighbourhoods = dfRating2.toPandas().iloc[:10, :]
topRatingBarcelonaNeighbourhoods.to_excel("topRatingBarcelonaNeighbourhoods.xlsx", index=False)

print('---------------SUCCESS---------------')
