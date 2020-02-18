# -*- coding: utf-8 -*-
"""
Created on Thu May 23 11:53:35 2019

@author: ASUS
"""

#importar librerias necesarias para cargar el archivo, convertirlo a un marco de datos espacial y crear las geometrias de punto
import pandas as pd
import geopandas as gpd 
from shapely.geometry import Point
import numpy as np
#carga el archivo
df=pd.read_csv('Encuesta_de_movilidad_de_Bogot__2015___Caracterizaci_n_viajes___Origen_Destino.csv')


pd.set_option('display.max_columns',100)
pd.set_option('display.max_colwidth',500)
pd.options.display.max_rows = 999
#deshacerse de las columnas no necesarias
df=df.drop(['FACTOR_AJUSTE',
       'PONDERADOR_CALIBRADO', 'DIA_HABIL', 'DIA_NOHABIL', 'PICO_HABIL',
       'PICO_NOHABIL', 'VALLE_NOHABIL', 'VALLE_HABIL', 'PI_K_I', 'PI_K_II',
       'PI_K_III', 'FE_TOTAL', 'FACTOR_AJUSTE_TRANSMILENIO',
       'PONDERADOR_CALIBRADO_VIAJES'], axis=1)
#ajustar tipo de dato de algunas columnas y valores nulos o ceros
df['TIEMPO_CAMINO'].fillna(0, inplace=True)
df['TIEMPO_CAMINO']=df['TIEMPO_CAMINO'].astype(int)
df['LATITUD_ORIGEN']=df['LATITUD_ORIGEN'].replace(0, np.nan)
df['LATITUD_ORIGEN']=df['LATITUD_ORIGEN'].fillna(method='ffill')
#int64_integer largo
df['LATITUD_ORIGEN']=df['LATITUD_ORIGEN'].astype('int64')
df['LATITUD_DESTINO']=df['LATITUD_DESTINO'].replace(0, np.nan)
df['LATITUD_DESTINO']=df['LATITUD_DESTINO'].fillna(method='ffill')
df['LATITUD_DESTINO']=df['LATITUD_DESTINO'].astype('int64')
#df['HORA_INICIO']=pd.to_timedelta(df.HORA_INICIO,unit='h')
#df['HORA_FIN']=pd.to_timedelta(df.HORA_FIN,unit='h')
#df['DIFERENCIA_HORAS']=pd.to_timedelta(df.DIFERENCIA_HORAS,unit='h')
print(df.columns)
print(df.shape)
print(df.head(10))
print(df.dtypes)
#df.to_csv('Encuesta_de_movilidad_de_Bogot__2015___Caracterizaci_n_viajes___Origen_Destino2.csv')
#%%

#conteo de medio de transporte
subdf=df[['MEDIO_PREDOMINANTE']]
counts_subdf=subdf.apply(pd.value_counts)
subdf= counts_subdf.fillna(0)
subdf['Medio'] = subdf.index
subdf.reset_index(inplace = True)
subdf = subdf[['Medio','MEDIO_PREDOMINANTE']]

print(subdf.sort_values(by=['MEDIO_PREDOMINANTE'], ascending=False).head(10))

#conteo zat de origen
subdf1=df[['ZAT_ORIGEN']]
counts_subdf1=subdf1.apply(pd.value_counts)
subdf1= counts_subdf1.fillna(0)
subdf1['zat'] = subdf1.index
subdf1.reset_index(inplace = True)
subdf1 = subdf1[['zat','ZAT_ORIGEN']]

print(subdf1.sort_values(by=['ZAT_ORIGEN'], ascending=False).head(10))

#conteo zat de destino
subdf2=df[['ZAT_DESTINO']]
counts_subdf2=subdf2.apply(pd.value_counts)
subdf2= counts_subdf2.fillna(0)
subdf2['zat'] = subdf2.index
subdf2.reset_index(inplace = True)
subdf2 = subdf2[['zat','ZAT_DESTINO']]

print(subdf2.sort_values(by=['ZAT_DESTINO'], ascending=False).head(10))
#%%
#seleccion de todos los regstros cuyo medio de transporte sea alguno modo de la red de transporte publico de bogota
df=df[(df.MEDIO_PREDOMINANTE=='TPC-SITP') | (df.MEDIO_PREDOMINANTE=='Transmilenio') | (df.MEDIO_PREDOMINANTE=='ALIMENTADOR')]
print(df.head(10))
print(df.shape)
#%%
#creacion de geometrias
#es importante que las coordenadas sean escritas con punto flotante o abrá problemas para representar
df['LONGITUD_ORIGEN']=df['LONGITUD_ORIGEN'].replace(0, np.nan)
df['LONGITUD_ORIGEN']=df['LONGITUD_ORIGEN'].fillna(method='ffill')
df['LONGITUD_ORIGEN'] = df['LONGITUD_ORIGEN'].astype(str)
df['LONGITUD_ORIGEN'] = (df['LONGITUD_ORIGEN'].str[:3] + '.' + df['LONGITUD_ORIGEN'].str[3:-2]).astype(float)



df['LATITUD_ORIGEN'] = df['LATITUD_ORIGEN'].astype(str)
df['LATITUD_ORIGEN'] = (df['LATITUD_ORIGEN'].str[:1] + '.' + df['LATITUD_ORIGEN'].str[1:-2]).astype(float)
print(df[['LONGITUD_ORIGEN','LATITUD_ORIGEN']].head(30))
print(df[['LONGITUD_ORIGEN','LATITUD_ORIGEN']].tail(30))
print(df.dtypes)

#buscar por cualquiera que tenga un nan
#print(df.loc[:,df.isnull().any()])
#%%
#crear la geometria
df['geometry'] = df.apply(lambda x: Point(float(x.LONGITUD_ORIGEN), float(x.LATITUD_ORIGEN)), axis=1)
print(df['geometry'].head())
#identificar el sistema de coordenadas
crs={'init': 'epsg:4326'}
#crear un marco de datos espacial
df_geo = gpd.GeoDataFrame(df,crs =crs, geometry = df.geometry)
print(df_geo.crs)
print(type(df_geo))
print(df_geo['geometry'])
#guardar en un shapefile las ubicaciones de origen
df_geo.to_file("Origen_viajes.shp")


#%%
#df_geo.to_file(driver = 'ESRI Shapefile', filename= "Origen_viajes.shp")
#df_geo1.to_file(driver = 'ESRI Shapefile', filename= "Destino_viajes.shp")

df1=df.copy()


df1['LONGITUD_DESTINO']=df1['LONGITUD_DESTINO'].replace(0, np.nan)
df1['LONGITUD_DESTINO']=df1['LONGITUD_DESTINO'].fillna(method='ffill')
df1['LONGITUD_DESTINO'] = df1['LONGITUD_DESTINO'].astype(str)
df1['LONGITUD_DESTINO'] = (df1['LONGITUD_DESTINO'].str[:3] + '.' + df1['LONGITUD_DESTINO'].str[3:-2]).astype(float)


df1['LATITUD_DESTINO'] = df1['LATITUD_DESTINO'].astype(str)
df1['LATITUD_DESTINO'] = (df1['LATITUD_DESTINO'].str[:1] + '.' + df1['LATITUD_DESTINO'].str[1:-2]).astype(float)
print(df1[['LONGITUD_DESTINO','LATITUD_DESTINO']].head())
print(df1[['LONGITUD_DESTINO','LATITUD_DESTINO']].tail())
print(df1.dtypes)
#%%

df1['geometry'] = df1.apply(lambda x: Point(float(x.LONGITUD_DESTINO), float(x.LATITUD_DESTINO)), axis=1)
df_geo1 = gpd.GeoDataFrame(df1,crs =crs, geometry = df1.geometry)
print(df_geo1.crs)
print(type(df_geo1))
print(df_geo['geometry'])
#guardar en un shapefile las ubicaciones de oestino
df_geo1.to_file("Destino_viajes.shp")