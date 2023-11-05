![](../images/proyecto_etl.png)

![1699118363074](image/README/1699118363074.png)![](../images/proyecto_etl.png)![](../images/proyecto_etl.png)

## Proyecto ETL

Requisitos generales

* Extraer datos de **3 fuentes diferentes**
* Extraer **datos usando 2 herramientas diferentes**
* El código debe guardarse en un archivo ejecutable de Python,
* Los datos deben guardarse en una carpeta llamada data o src.
* Incluir un archivo README.md que describa los pasos que siguió

## Comenzamos extrayendo datos de 3 fuentes diferentes:

## Primer fuente: Wikipedia

Utilizo la primera herramienta de extracción, el Web Scrapping

```py
from selenium import webdriver
from selenium.webdriver.common.by import By

# fecha confinamiento
url= 'https://es.wikipedia.org/wiki/Pandemia_de_COVID-19_en_Espa%C3%B1a'

driver = webdriver.Chrome()   
driver.get('https://www.google.es')
driver.get(url)
driver.find_elements(By.XPATH, '//*[@id="mw-content-text"]/div[1]/table[2]/tbody/tr[13]/td')[0].text.split("\n")[1:]

['15 de marzo-21 de junio (98 días)']
```

## Segunda Fuente: Página web del Instituto Nacional de Estadística

Bibliotecas necesarias para la limpieza y transformación de datos

```py
import pandas as pd
pd.set_option('display.max_columns', None)

import numpy as np

import warnings
warnings.filterwarnings('ignore')
```

## Descarga de archivos CSV de INE (https://www.ine.es/index.htm)

## Pasos que seguí para la limpieza del archivo descargado, llamado INE

### Leo el archivo

```py
INE_ori = pd.read_csv('../data/INE.csv', delimiter=";", encoding='latin1') #leo el archivo "INE"
INE = INE_ori.copy() #hago una copia
```

### Elimino todas las columnas que no me sirven

```py
columnas_a_eliminar = ['Total Nacional', 'Comunidades y Ciudades Autónomas', 'Edad de la madre', 'Tipo de dato',]
INE = INE.drop(columnas_a_eliminar, axis=1)
```

### Divido la columna "Periodo" en "año" y "mes"

```py
INE[['año', 'mes']] = INE['Periodo'].str.split('M', expand=True)
INE = INE.drop('Periodo', axis=1)
INE.head()
```

### Ordeno las filas cronológicamente

```py
INE = INE.sort_values(by=['año', 'mes']) 
INE = INE.reset_index(drop=True)
```

### Reordeno las columnas

```py
INE = INE[['mes', 'año', 'Total']] 
```

## Tercera Fuente: Portal de datos abiertos del Ayuntamiento de Madrid

## Descargo archivos de Portal de datos abiertos del Ayuntamiento de Madrid (https://datos.madrid.es/)

### Concateno los cuatro archivos de contaminación sonora

```py
archivos_csv = ["../data/Anio2019.csv", "../data/Anio2020.csv", "../data/Anio2021.csv", "../data/Anio2022.csv"]

# Lista para almacenar DataFrames de los archivos CSV
dataframes = []

# Lee y carga cada archivo CSV en un DataFrame
for archivo in archivos_csv:
    df = pd.read_csv(archivo, delimiter=";", encoding='latin1')
    dataframes.append(df)

# Concatena los DataFrames en uno solo
resultado = pd.concat(dataframes, ignore_index=True)

# Guarda el resultado en un nuevo archivo CSV
resultado.to_csv("../data/contaminacion.csv", index=False, sep=";")
```

### Pasos que seguí para la limpieza del nuevo archivo "contaminacion"

### Leo el archivo

```py
contaminacion_ori = pd.read_csv('../data/contaminacion.csv', delimiter=";", encoding='latin1') #leo el archivo "contaminacion"
contaminacion = contaminacion_ori.copy() #hago una copia
```

### Elimino todas las columnas que no me sirven

```py
columnas_a_eliminar = ["Ld", "Le", "Ln", "LAS01", "LAS10", "LAS50", "LAS90", "LAS99", "Nombre", "NMT", "SituaciÃ¯Â¿Â½n"]

contaminacion = contaminacion.drop(columnas_a_eliminar, axis=1) 
```

### Renombro la columna de medida de contaminación acústica

```py
contaminacion = contaminacion.rename(columns={'LAeq24': 'contam_media'})
```

### Elimino las filas con N/D

```py
contaminacion = contaminacion[contaminacion['contam_media'] != "N/D"] 
```

### Divido la columna "Fecha"

```py
contaminacion[['mes', 'año']] = contaminacion['Fecha'].str.split('-', expand=True) #divide la columna

# Mapea los valores del mes a sus equivalentes en número
meses = {
    'ene': '01',
    'feb': '02',
    'mar': '03',
    'abr': '04',
    'may': '05',
    'jun': '06',
    'jul': '07',
    'ago': '08',
    'sep': '09',
    'oct': '10',
    'nov': '11',
    'dic': '12'}

contaminacion['mes'] = contaminacion['mes'].map(meses)
contaminacion['año'] = '20' + contaminacion['año']  # Agrega "20" antes del año
contaminacion = contaminacion.drop('Fecha', axis=1)  # Elimina la columna original "Fecha" 
```

### Calculo la media por mes:

```py
#Convierte la columna a tipo numérico 
contaminacion['contam_media'] = contaminacion['contam_media'].str.replace(',', '.', regex=True).astype(float) 
# Agrupa por mes y calcula la media
contaminacion = contaminacion.groupby(['mes', 'año'])['contam_media'].mean().reset_index().round(1).astype(str) 
```

### Ordeno las filas cronológicamente

```py
contaminacion = contaminacion.sort_values(by=['año', 'mes_numero']) 
contaminacion = contaminacion.reset_index(drop=True)
```

### Reordeno las columnas

```py
contaminacion = contaminacion[['mes', 'año', 'contam_media']]
```





RESULTADOS



CREACION DE BASE DE DATOS

```sql
CREATE DATABASE etl;
```

```sql

CREATE TABLE etl.pandemia (	id INT auto_increment NOT NULL,	mes INT NOT NULL,	año INT NOT NULL,	cuarentena VARCHAR(10) NOT NULL,	CONSTRAINT pandemia_PK PRIMARY KEY (id))ENGINE=InnoDBDEFAULT CHARSET=utf8mb4COLLATE=utf8mb4_0900_ai_ci;
```

Creación de las tablas nacimientos y contaminacion desde los notebooks

```py

from sqlalchemy import create_engine
str_conn = f'mysql+pymysql://root:Joaquin01@localhost:3306/etl'
cursor = create_engine(str_conn)

INE.to_sql(name='nacimientos',       # nombre de la tabla

con=cursor,           # conexion al servidor

if_exists='replace',  # replace sobreescribe la tabla

index=True)

contaminacion.to_sql(name='contaminacion',       # nombre de la tabla

con=cursor,           # conexion al servidor

if_exists='replace',  # replace sobreescribe la tabla

index=True)
```


INSERTAR DATOS EN TABLA PANDEMIA

```sql
INSERT INTO etl.pandemia (id,mes,año,cuarentena) VALUES	 (0,'01','2019','False'),	 (1,'02','2019','False'),	 (2,'03','2019','False'),	 (3,'04','2019','False'),	 (4,'05','2019','False'),	 (5,'06','2019','False'),	 (6,'07','2019','False'),	 (7,'08','2019','False'),	 (8,'09','2019','False'),	 (9,'10','2019','False'),	 (10,'11','2019','False'),	 (11,'12','2019','False'),	 (12,'01','2020','False'),	 (13,'02','2020','False'),	 (14,'03','2020','True'),	 (15,'04','2020','True'),	 (16,'05','2020','True'),	 (17,'06','2020','True'),	 (18,'07','2020','False'),	 (19,'08','2020','False'),	 (20,'09','2020','False'),	 (21,'10','2020','False'),	 (22,'11','2020','False'),	 (23,'12','2020','False'),	 (24,'01','2021','False'),	 (25,'02','2021','False'),	 (26,'03','2021','False'),	 (27,'04','2021','False'),	 (28,'05','2021','False'),	 (29,'06','2021','False'),	 (30,'07','2021','False'),	 (31,'08','2021','False'),	 (32,'09','2021','False'),	 (33,'10','2021','False'),	 (34,'11','2021','False'),	 (35,'12','2021','False'),	 (36,'01','2022','False'),	 (37,'02','2022','False'),	 (38,'03','2022','False'),	 (39,'04','2022','False'),	 (40,'05','2022','False'),	 (41,'06','2022','False'),	 (42,'07','2022','False'),	 (43,'08','2022','False'),	 (44,'09','2022','False'),	 (45,'10','2022','False'),	 (46,'11','2022','False'),	 (47,'12','2022','False');
```


ASIGNAR PK A NACIIMIENTO

```sql

ALTER TABLE etl.nacimientos ADD CONSTRAINT nacimientos_PK PRIMARY KEY (index);
```

ASIGNAR PK A CONTAMINACION

```sql

ALTER TABLE etl.contaminacion ADD CONSTRAINT contaminacion_PK PRIMARY KEY (index);
```
