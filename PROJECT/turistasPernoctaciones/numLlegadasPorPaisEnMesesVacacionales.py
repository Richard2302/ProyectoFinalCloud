import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
import pandas as pd
import matplotlib.pyplot as plt


conf = SparkConf().setAppName('turistasPernoctaciones')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "turistas_pernoctaciones.csv"

df = spark.read.csv(path, inferSchema = True, header = True)

df = df.filter(df["Concepto turístico"] == "Turistas")
dfContinentes = df.select(df['Continente y país de residencia: Nivel 2'].alias('CONTINENTE'),df['Periodo'][6:2].alias('Mes'), df.Total)
dfContinentes.na.drop(how="any")
dfContinentes = dfContinentes.withColumn("Total", dfContinentes["Total"].cast("int"))

# DF EUROPA

dfEuropa = dfContinentes.filter(f.col('CONTINENTE') == "Europa")

dfEuropa = dfEuropa.drop("CONTINENTE")
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

totalMesesEuropa = []
for mes in meses:
	totalMesDF = dfEuropa.filter(f.col('Mes') == mes).agg({'Total': 'sum'})
	totalMesesEuropa.append(totalMesDF.collect()[0][0])


datosEuropa = {
	"meses"    : meses,
	"Total" : totalMesesEuropa
}

df = pd.DataFrame(datosEuropa)
print(df)

#VOLCAR LOS RESULTADOS EN UN EXCEL
df.to_excel("llegadas_españa_europa_por_meses.xlsx", index=False)

#GRAFICO 
fig, ax = plt.subplots()
ax.bar(meses, totalMesesEuropa)
plt.show()



# DF AFRICA

dfAfrica = dfContinentes.filter(f.col('CONTINENTE') == "África")

dfAfrica = dfAfrica.drop("CONTINENTE")

totalMesesAfrica = []
for mes in meses:
	totalMesDF = dfAfrica.filter(f.col('Mes') == mes).agg({'Total': 'sum'})
	totalMesesAfrica.append(totalMesDF.collect()[0][0])


datosAfrica = {
	"meses"    : meses,
	"Total" : totalMesesAfrica
}

df = pd.DataFrame(datosAfrica)
print(df)

#VOLCAR LOS RESULTADOS EN UN EXCEL
df.to_excel("llegadas_españa_africa_por_meses.xlsx", index=False)


#GRAFICO 
fig, ax = plt.subplots()
ax.bar(meses, totalMesesAfrica)
plt.show()


# DF AMÉRICA

dfAmerica = dfContinentes.filter(f.col('CONTINENTE') == "América")

dfAmerica = dfAmerica.drop("CONTINENTE")

totalMesesAmerica = []
for mes in meses:
	totalMesDF = dfAmerica.filter(f.col('Mes') == mes).agg({'Total': 'sum'})
	totalMesesAmerica.append(totalMesDF.collect()[0][0])


datosAmerica = {
	"meses"    : meses,
	"Total" : totalMesesAmerica
}

df = pd.DataFrame(datosAmerica)
print(df)

#VOLCAR LOS RESULTADOS EN UN EXCEL
df.to_excel("llegadas_españa_america_por_meses.xlsx", index=False)

#GRAFICO 
fig, ax = plt.subplots()
ax.bar(meses, totalMesesAmerica)
plt.show()

# DF ASIA

dfAsia = dfContinentes.filter(f.col('CONTINENTE') == "Asia")

dfAsia = dfAsia.drop("CONTINENTE")

totalMesesAsia = []
for mes in meses:
	totalMesDF = dfAsia.filter(f.col('Mes') == mes).agg({'Total': 'sum'})
	totalMesesAsia.append(totalMesDF.collect()[0][0])


datosAsia = {
	"meses"    : meses,
	"Total" : totalMesesAsia
}

df = pd.DataFrame(datosAsia)
print(df)

#VOLCAR LOS RESULTADOS EN UN EXCEL
df.to_excel("llegadas_españa_asia_por_meses.xlsx", index=False)

#GRAFICO 
fig, ax = plt.subplots()
ax.bar(meses, totalMesesAsia)
plt.show()


# DF OCEANIA

dfOceania = dfContinentes.filter(f.col('CONTINENTE') == "Oceanía")

dfOceania = dfOceania.drop("CONTINENTE")
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

totalMesesOceania = []
for mes in meses:
	totalMesDF = dfOceania.filter(f.col('Mes') == mes).agg({'Total': 'sum'})
	totalMesesOceania.append(totalMesDF.collect()[0][0])


datosOceania = {
	"meses"    : meses,
	"Total" : totalMesesOceania
}

df = pd.DataFrame(datosOceania)
print(df)

#VOLCAR LOS RESULTADOS EN UN EXCEL
df.to_excel("llegadas_españa_oceania_por_meses.xlsx", index=False)

#HACER EL GRAFICO

#GRAFICO NO HAY DATOS
