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
df.to_excel("salida_airbnb_madrid.xlsx", index=False)

#HACER EL GRAFICO

fig, ax = plt.subplots()

ax.plot(meses, mesLibres, color = 'tab:green', label = "Libres")
ax.plot(meses, mesOcupados, color = 'tab:red', label = "Ocupados")
ax.set_title('Airbnbs libres/Ocupados Madrid por mes año 2019/2020', loc = "left", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.set_xlabel("MESES", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.set_ylabel("Nº Airbnbs", fontdict = {'fontsize':14, 'fontweight':'bold', 'color':'tab:blue'})
ax.legend(loc = 'upper right')
plt.show()
