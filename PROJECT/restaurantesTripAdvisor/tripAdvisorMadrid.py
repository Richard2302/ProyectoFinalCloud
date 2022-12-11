import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
import pandas as pd
import matplotlib.pyplot as plt


conf = SparkConf().setAppName('valoracionesTripAdvisorMadrid')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "trip_advisor_madrid.csv"

df = spark.read.option("header", "true").csv(path)
df = df.select(df["restaurant_name"], df["rating_review"], df["url_restaurant"])

#df.agg({'rating_review': 'avg'}).show()

df = df.withColumn("rating_review", df["rating_review"].cast("int"))

df = df.groupBy('restaurant_name').avg("rating_review").sort("restaurant_name")

df = df.withColumnRenamed("avg(rating_review)", "Media Valoracion")


#SACAMOS LA CANTIDAD DE RESTAURANTES ENTRE LOS RANGOS 1-2, 2-3, 3-4, 4-5
cantidadRest1_2 = df.filter((1 <= df["Media Valoracion"]) & (df["Media Valoracion"] < 2)).count()
cantidadRest2_3 = df.filter((2 <= df["Media Valoracion"]) &  (df["Media Valoracion"] < 3)).count()
cantidadRest3_4 = df.filter((3 <= df["Media Valoracion"]) &  (df["Media Valoracion"] < 4)).count()
cantidadRest4_5 = df.filter((4 <= df["Media Valoracion"]) &  (df["Media Valoracion"] <= 5)).count()



dfMejorValorados = df.filter(df["Media Valoracion"] > 4.8)
dfMejorValorados = dfMejorValorados.withColumnRenamed("restaurant_name", "Restaurantes Mejor Valorados")
dfPeorValorados  = df.filter(df["Media Valoracion"] < 2)
dfPeorValorados = dfPeorValorados.withColumnRenamed("restaurant_name", "Restaurantes Peor Valorados")
dfMejorValorados.show()
dfPeorValorados.show()

#VOLCAR LOS RESULTADOS EN UN EXCEL
dfMejorValorados.toPandas().to_excel("restaurantes_mejor_valorados_madrid.xlsx", index=False)
dfPeorValorados.toPandas().to_excel("restaurantes_peor_valorados_madrid.xlsx", index=False)


#HACER GRAFICO

fig, ax = plt.subplots()
nombres = ["1 A 2 ESTRELLAS","2 A 3 ESTRELLAS","3 A 4 ESTRELLAS","4 A 5 ESTRELLAS"]
colores = ["#EE6055","#60D394","#AAF683","#FFD97D","#FF9B85"]
ax.pie([cantidadRest1_2, cantidadRest2_3, cantidadRest3_4, cantidadRest4_5], labels=nombres, autopct="%0.1f %%", colors=colores)
plt.title("Valoraciones Madrid")
plt.show()


