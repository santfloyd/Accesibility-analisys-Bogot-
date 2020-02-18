# -*- coding: utf-8 -*-
"""
Created on Wed May 29 17:27:48 2019

@author: ASUS
"""

#importar librerias necesarias para cargar el archivo, convertirlo a un marco de datos espacial y crear las geometrias de punto
import pandas as pd
#from sqlalchemy import create_engine 
import numpy as np

pd.set_option('display.max_columns',100)
pd.set_option('display.max_colwidth',500)
pd.options.display.max_rows = 999


#engine = create_engine('postgresql://postgres:POSTGRES@localhost:5432/postgres')


Hora_inicio=['00:00:00','04:00:00','06:00:00','09:00:00','15:00:00','19:00:00','23:00:00','04:00:00','08:00:00','20:00:00','00:00:00','05:00:00','09:00:00','18:00:00']
Hora_final=['03:59:00','05:59:00','08:59:00','14:59:00','18:59:00','22:59:00','03:59:00','07:59:00','19:59:00','23:59:00','04:59:00','08:59:00','17:59:00','23:59:00']
Periodo=['Tarde en la noche','temprano','hora pico mañana','medio día','hora pico tarde','noche','Tarde en la noche','Sabado mañana','Sabado medio día','Sabado noche','Domingo noche','Domingo mañana','Domingo medio día','Domingo noche']
Dia_semana=['entre semana','entre semana','entre semana','entre semana','entre semana','entre semana','entre semana','Sabado','Sabado','Sabado','Domingo','Domingo','Domingo','Domingo']
lapso=[1,2,3,6,4,4,1,4,12,4,7,4,9,7]
df0=pd.DataFrame()
df0['Hora_inicio'] = pd.Series(Hora_inicio)
df0['Hora_final'] = pd.Series(Hora_final)
df0['Periodo'] = pd.Series(Periodo)
df0['Dia_semana'] = pd.Series(Dia_semana)
df0['lapso'] = pd.Series(lapso)
time_format = '%H:%M:%S'
df0['Hora_inicio']=pd.to_timedelta(df0['Hora_inicio'], unit='s')
df0['Hora_final']=pd.to_timedelta(df0['Hora_final'], unit='s')
#df0.to_sql(name='inputs', con=engine, if_exists='replace',index=True)	


df=pd.read_csv("gtfs_bogota_agency.csv",sep=',')
#df.to_sql(name='gtfs_bogota_agency', con=engine, if_exists='replace',index=True)



df1=pd.read_csv('gtfs_bogota_calendar.csv',sep=',')
tipo_dia=['entre semana','Domingo','entre semana','entre semana','entre semana','Sabado']
df1['tipo_dia']=pd.Series(tipo_dia)
#df1.to_sql(name='gtfs_bogota_calendar', con=engine, if_exists='replace',index=True)



df2=pd.read_csv("gtfs_bogota_calendar_dates.csv",sep=',')
#df2.to_sql(name='gtfs_bogota_calendar_dates', con=engine, if_exists='replace',index=True)


df3=pd.read_csv("gtfs_bogota_routes.csv",sep=',')
df3['agency_id']=df3['agency_id'].astype(str)
df3['route_type']=df3['route_type'].astype(str)
df3['route_short_name']=df3['route_short_name'].astype(str)

#df3.to_sql(name='gtfs_bogota_routes', con=engine, if_exists='replace',index=True)

df4=pd.read_csv("gtfs_bogota_shapes.csv",sep=',')
#df4.to_sql(name='gtfs_bogota_shapes', con=engine, if_exists='replace',index=True)


df6=pd.read_csv("gtfs_bogota_stops.csv",sep=',')
#df6.to_sql(name='gtfs_bogota_stops', con=engine, if_exists='replace',index=True)


df7=pd.read_csv("gtfs_bogota_trips.csv",sep=',')
df7=pd.merge(df7,df3[['route_id','route_short_name']],left_on=['route_id'], right_on = ['route_id'], how = 'left')
df7=pd.merge(df7,df1[['service_id','tipo_dia']],left_on=['service_id'], right_on = ['service_id'], how = 'left')
#df7.to_sql(name='gtfs_bogota_trips', con=engine, if_exists='replace',index=True)


df5=pd.read_csv("gtfs_bogota_stop_times.csv",sep=',')
df5['arrival_time']=pd.to_timedelta(df5['arrival_time'], unit='s')
df5['departure_time']=pd.to_timedelta(df5['departure_time'], unit='s')
df5=pd.merge(df5,df7[['trip_id','route_short_name','service_id','tipo_dia']],left_on=['trip_id'], right_on = ['trip_id'], how = 'left')
conditions = [
            (df5['tipo_dia'] == 'entre semana') & (df5['arrival_time'] >= df0.loc[0,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[0,'Hora_final']),
            (df5['tipo_dia'] == 'entre semana') & (df5['arrival_time'] >= df0.loc[1,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[1,'Hora_final']),
            (df5['tipo_dia'] == 'entre semana') & (df5['arrival_time'] >= df0.loc[2,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[2,'Hora_final']),
            (df5['tipo_dia'] == 'entre semana') & (df5['arrival_time'] >= df0.loc[3,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[3,'Hora_final']),
            (df5['tipo_dia'] == 'entre semana') & (df5['arrival_time'] >= df0.loc[4,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[4,'Hora_final']),
            (df5['tipo_dia'] == 'entre semana') & (df5['arrival_time'] >= df0.loc[5,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[5,'Hora_final']),
            (df5['tipo_dia'] == 'entre semana') & (df5['arrival_time'] >= df0.loc[6,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[6,'Hora_final']),
            (df5['tipo_dia'] == 'Sabado') & (df5['arrival_time'] >= df0.loc[7,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[7,'Hora_final']),
            (df5['tipo_dia'] == 'Sabado') & (df5['arrival_time'] >= df0.loc[8,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[8,'Hora_final']),
            (df5['tipo_dia'] == 'Sabado') & (df5['arrival_time'] >= df0.loc[9,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[9,'Hora_final']),
            (df5['tipo_dia'] == 'Domingo') & (df5['arrival_time'] >= df0.loc[10,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[10,'Hora_final']),
            (df5['tipo_dia'] == 'Domingo') & (df5['arrival_time'] >= df0.loc[11,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[11,'Hora_final']),
            (df5['tipo_dia'] == 'Domingo') & (df5['arrival_time'] >= df0.loc[12,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[12,'Hora_final']),
            (df5['tipo_dia'] == 'Domingo') & (df5['arrival_time'] >= df0.loc[13,'Hora_inicio']) & (df5['arrival_time'] <= df0.loc[13,'Hora_final']),
            
            ]   
choices = [df0.loc[0,'Periodo'],df0.loc[1,'Periodo'],df0.loc[2,'Periodo'],df0.loc[3,'Periodo'],df0.loc[4,'Periodo'],df0.loc[5,'Periodo'],df0.loc[6,'Periodo'],df0.loc[7,'Periodo'],df0.loc[8,'Periodo'],df0.loc[9,'Periodo'],
           df0.loc[10,'Periodo'],df0.loc[11,'Periodo'],df0.loc[12,'Periodo'],df0.loc[13,'Periodo']]
df5['Periodo'] = np.select(conditions, choices)
#print(df5.loc[df5['Periodo']=='noche'])
#df5.to_sql(name='gtfs_bogota_stop_times', con=engine, if_exists='replace',index=True)




#%%

freq=df5.groupby(['route_short_name','tipo_dia']).agg({'arrival_time':'min', 'departure_time':'max'})[['arrival_time','departure_time']].reset_index()
print(freq)
#print(df5.stack(level='tipo_dia'))

#%%
df6.to_excel("stops.xls",sheet_name='Stops')



