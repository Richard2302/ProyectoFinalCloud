import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
import pandas as pd
import matplotlib.pyplot as plt


conf = SparkConf().setAppName('cantidadGastadaPorAnio')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "llegadasTuristasVisitantesYExpenditure.csv"

df = spark.read.option("header", "true").csv(path)
df = df.select(df["Country"], df["Year"], df["Series"].alias("CANTIDAD GASTADA / LLEGADAS"), df["Value"])
df = df.withColumn("Value", df["Value"].cast("int"))
df = df.withColumn("Year", df["Year"].cast("int"))

dfCantGastada = df.filter(df["CANTIDAD GASTADA / LLEGADAS"] == "Tourism expenditure (millions of US dollars)")
dfCantGastada = dfCantGastada.drop("CANTIDAD GASTADA / LLEGADAS")
dfCantGastada = dfCantGastada.filter(dfCantGastada["Year"] > 2010)

#AÑOS 2018, 2019, 2020
dfCantGastada = dfCantGastada.groupBy("Country", "Year").sum("Value").sort("Year")
dfCantGastada = dfCantGastada.na.drop(how="any")
dfCantGastada.show()

dfCantGastada.toPandas().to_excel("millonesGastados.xlsx", index=False)

#AÑO 2018
df2018 = dfCantGastada.filter(dfCantGastada["Year"] == 2018)
df2018 = df2018.drop("Year")


dfPandas2018 = df2018.toPandas().to_numpy().tolist()

cantGastada2018= []

for pais in dfPandas2018:
	cantGastada2018.append(pais[1])	

#AÑO 2019
df2019 = dfCantGastada.filter(dfCantGastada["Year"] == 2019)
df2019 = df2019.drop("Year")


dfPandas2019 = df2019.toPandas().to_numpy().tolist()

cantGastada2019= []

for pais in dfPandas2019:
	cantGastada2019.append(pais[1])	

#AÑO 2020
df2020 = dfCantGastada.filter(dfCantGastada["Year"] == 2020)
df2020 = df2020.drop("Year")


dfPandas2020 = df2020.toPandas().to_numpy().tolist()

cantGastada2020= []

for pais in dfPandas2020:
	cantGastada2020.append(pais[1])	

#HACER EL GRAFICO

fig, ax = plt.subplots()

ax.plot(cantGastada2018, color = 'tab:green', label = "2018")
ax.set_title('Millones € Gastados por pais en 2018', loc = "left", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.set_xlabel("PAISES", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.set_ylabel("Millones de € gastados", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.legend(loc = 'upper right')
plt.show()


fig, ax = plt.subplots()
ax.plot(cantGastada2019, color = 'tab:red', label = "2019")
ax.set_title('Millones € Gastados por pais en 2019', loc = "left", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.set_xlabel("PAISES", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.set_ylabel("Millones de € gastados", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.legend(loc = 'upper right')
plt.show()


fig, ax = plt.subplots()
ax.plot(cantGastada2020, color = 'tab:blue', label = "2020")
ax.set_title('Millones € Gastados por pais en 2020', loc = "left", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.set_xlabel("PAISES", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.set_ylabel("Millones de € gastados", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.legend(loc = 'upper right')
plt.show()
