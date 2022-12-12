# Proyecto FINAL CLOUD
# BIG TURISMO

Ofrecemos distintas comparativas para que el cliente pueda elaborar sus distintas conclusiones a la hora de decidir en cómo y cuándo enfocar la apertura de un nuevo negocio.

# // LOCAL

1.- sudo apt install python3-pip (Instalar pip para posteriormente poder instalar las librerias de pandas y matplotlib y poder acceder a las funcionalidades que estas ofrecen)
2. pip3 install pandas (para instalar la libreria pandas)
3. pip3 install matplotlib (para instalar la libreria matplotlib para dibujar los graficos)
4. Tener pyspark instalado. 
5. Ubicar los dataset del drive compartido en /PROJECT/Datasets en las cada una de las carpetas donde se utilizan para que el script reconozca el path.
6. Ejecutar el script mediante spark-submit "nombre_Archivo.py" 

# // EN CLOUD 

1. Crear un cluster (region zurich west 6) dataproc
2. Crear  un bucket
3. Cambiar la ruta del path en los scripts poniendo la ubicacion del csv en el bucket.
4. Subir todos los .py y los datasets comentando todo lo relacionado con pandas (toExcel ) y lo del matplotlib
5. Iniciar un nuevo trabajo para cada script.
6. Para cada trabajo indicar el script principal y en argumentos especificar la ruta del csv (dataset) en el bucket.



# Equipo

Jorge Morales López, 
Santiago Marcitllach Arias, 
Victor Moreno Pérez, 
Ricardo Carazo Pérez, 
Jose Otegui Marín
