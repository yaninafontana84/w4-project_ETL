# w4-project_ETL

# Importar librerias

```py
import pandas as pd
pd.set_option('display.max_columns', None)

import numpy as np

import warnings
warnings.filterwarnings('ignore')

```
# Web Scraping
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
# descargar archivos CSV de INE (https://www.ine.es/index.htm)



# Descargo archivos de Portal de datos abiertos del Ayuntamiento de Madrid (https://datos.madrid.es/)

# Concateno los 4 años de contaminación

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

# Limpieza del nuevo archivo "contaminacion"
# Eliminación de todas las columnas que no nos sirven

```py
columnas_a_eliminar = ["Ld", "Le", "Ln", "LAS01", "LAS10", "LAS50", "LAS90", "LAS99", "Nombre", "NMT", "SituaciÃ¯Â¿Â½n"]

contaminacion = contaminacion.drop(columnas_a_eliminar, axis=1) 
```
# Renombro la columna de medida de contaminación acústica
```py
contaminacion = contaminacion.rename(columns={'LAeq24': 'contam_media'})
```

# Elimino las filas con N/D 
```py
contaminacion = contaminacion[contaminacion['contam_media'] != "N/D"] 
```

# División de la columna "Fecha"
```py
contaminacion[['mes', 'año']] = contaminacion['Fecha'].str.split('-', expand=True) #divide la columna
meses = { # Mapea los valores del mes a sus nombres abreviados
    'ene': 'enero',
    'feb': 'febrero',
    'mar': 'marzo',
    'abr': 'abril',
    'may': 'mayo',
    'jun': 'junio',
    'jul': 'julio',
    'ago': 'agosto',
    'sep': 'septiembre',
    'oct': 'octubre',
    'nov': 'noviembre',
    'dic': 'diciembre'}
contaminacion['mes'] = contaminacion['mes'].map(meses) 
contaminacion['año'] = '20' + contaminacion['año']  # Agrega "20" antes del año
contaminacion = contaminacion.drop('Fecha', axis=1)  # Elimina la columna original "Fecha" 
```

# Cálculo de la media por mes:
```py
#Convierte la columna a tipo numérico 
contaminacion['contam_media'] = contaminacion['contam_media'].str.replace(',', '.', regex=True).astype(float) 
# Agrupa por mes y calcula la media
contaminacion = contaminacion.groupby(['mes', 'año'])['contam_media'].mean().reset_index().round(1).astype(str) 
```
# Ordeno cronológicamente
```py
meses_dict = {
    'enero': 1,
    'febrero': 2,
    'marzo': 3,
    'abril': 4,
    'mayo': 5,
    'junio': 6,
    'julio': 7,
    'agosto': 8,
    'septiembre': 9,
    'octubre': 10,
    'noviembre': 11,
    'diciembre': 12}
contaminacion['mes_numero'] = contaminacion['mes'].map(meses_dict)
contaminacion = contaminacion.sort_values(by=['año', 'mes_numero']) 
contaminacion = contaminacion.reset_index(drop=True)
contaminacion.drop('mes_numero', axis=1, inplace=True)  # Elimina la columna temporal
```






