import pandas as pd
import io
import requests
import datetime
import mysql.connector
import pymysql

## Extraccion

## Carga de archivo local

df = pd.read_csv('municipio.csv')
#print("Se leyo el archivo municipio.csv, almacenado localmente")
print ("Carga de archivo local")
print (df.shape)
## Carga de archivo en la web

url = "https://ay-disco.s3.us-east-2.amazonaws.com/global_calificacion.csv"
s = requests.get(url).content
df2 = pd.read_csv(io.StringIO(s.decode('utf-8')))
#print("Se leyo el archivo global_calificacion.csv, almacenado remotamente")
print("Carga de archivo en la web")
print(df2.shape)

## Limpieza y transformacion

#Remover datos duplicados

print("Remover datos duplicados")
df = df.drop_duplicates();
df2 = df2.drop_duplicates();
print(df.shape)
print(df2.shape)

print("Utilización de datos de Guatemala")
df2 = df2[df2['Country'].str.lower() == 'guatemala']


# ELIMINAR COLUMNAS INNECESARIAS PARA EL ANÁISIS
df2 = df2.drop(columns=['Country_code'], axis=1)
df2 = df2.drop(columns=['WHO_region'], axis=1)

print(df.shape)
print(df2.shape)



#df2 = df2[df2['Date_reported'] > datetime.datetime(2020,1,1) ]

# FILTRAR POR FECHAS DEL AÑO 2020
#dfRemoto = df2[df2['Date_reported'].apply(lambda x: pd.to_datetime(x).year) == 2020]

print("-----------------------------------------------------------------")

print("Dataframes despues de limpieza")

print(df)
print(df2)


# Conexion mysql

# Connect to MySQL

#cnx = mysql.connector.connect(user='dba', password='USAC2024$',
#                              host='127.0.0.1', database='DataSet',
#                              auth_plugin='mysql_native_password')

#


# Database connection
connection = pymysql.connect(
    host="127.0.01",
    user="dba",
    password="USAC2024$",
    database="DataSet"
)


#recorrido por bloques

numero_bloque = 1


# Iterate in groups of 50 rows
tamanio_bloque = 50
for i in range(0, len(df), tamanio_bloque):
    group = df.iloc[i:i + tamanio_bloque]
    print(f"Bloque local  {numero_bloque}")
    numero_bloque = numero_bloque + 1
    #print(f"Processing rows {i} to {i + len(group) - 1}")
    #print(group)

print("----------------------------------------------------")
numero_bloque = 1

for i in range(0, len(df2), tamanio_bloque):
    group = df2.iloc[i:i + tamanio_bloque]
    print(f"Bloque remoto  {numero_bloque}")
    numero_bloque = numero_bloque + 1

    #print(f"Processing rows {i} to {i + len(group) - 1}")
    #print(group)


try:
    # Start a transaction
    with connection.cursor() as cursor:
        # Example Query 1: Insert data
       # sql1 = "INSERT INTO your_table (column1, column2) VALUES (%s, %s)"
       # cursor.execute(sql1, ("value1", "value2"))

        # Example Query 2: Update data
        #sql2 = "UPDATE your_table SET column2 = %s WHERE column1 = %s"
        #cursor.execute(sql2, ("updated_value", "value1"))

        # Commit the transaction
        connection.commit()
        print("Transaction committed successfully!")

except Exception as e:
    # Rollback the transaction in case of an error
    connection.rollback()
    print("Transaction failed! Rolled back.")
    print(f"Error: {e}")

finally:
    # Close the connection
    connection.close()
