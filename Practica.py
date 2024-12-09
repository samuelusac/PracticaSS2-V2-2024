import pandas as pd
import io
import requests

## Extraccion

## Archivo local

df = pd.read_csv('municipio.csv')
print("Se leyo el archivo municipio.csv, almacenado localmente")
print (df.shape)
## Archivo remoto

url = "https://ay-disco.s3.us-east-2.amazonaws.com/global_calificacion.csv"
s = requests.get(url).content
df2 = pd.read_csv(io.StringIO(s.decode('utf-8')))
print("Se leyo el archivo global_calificacion.csv, almacenado remotamente")
print(df2.shape)

