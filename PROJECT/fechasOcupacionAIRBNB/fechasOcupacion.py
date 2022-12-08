import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
import pandas as pd
import matplotlib.pyplot as plt


conf = SparkConf().setAppName('fechasOcupacion')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "airbnb_madrid.csv"

dfMadrid = spark.read.option("header", "true").csv(path)

dfMadrid = dfMadrid.select(dfMadrid['listing_id'].alias('id'),dfMadrid['date'][6:2].alias('Mes'), dfMadrid.available, dfMadrid.adjusted_price)



meses = ["09", "10", "11", "12", "01", "02", "03", "04", "05", "06", "07", "08"]

mesLibres   = []
mesOcupados = []
for mes in meses:
	mesLibre    = dfMadrid.filter(f.col('Mes') == mes).filter(f.col('available') == "t").count()
	mesOcupado  = dfMadrid.filter(f.col('Mes') == mes).filter(f.col('available') == "f").count()
	mesLibres.append(mesLibre)
	mesOcupados.append(mesOcupado)

datos = {
	"meses"    : meses,
	"ocupados" : mesOcupados,
	"libres"   : mesLibres 
}

df = pd.DataFrame(datos)
print(df)

#VOLCAR LOS RESULTADOS EN UN EXCEL
df.to_excel("salida_airbnb.xlsx", index=False)

#HACER EL GRAFICO

plt.plot(meses, mesLibres)
plt.xlabel("Meses")
plt.ylabel("Dias")
plt.show()



