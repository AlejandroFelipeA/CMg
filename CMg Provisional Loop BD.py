#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
import pymysql
from functools import reduce
import time
import numpy as np
from datetime import datetime
from datetime import date
from datetime import timedelta
#import datetime as dt
import locale
locale.setlocale(locale.LC_ALL, 'es_ES.utf8')


# In[187]:


main_path = os.path.abspath('')
main_path


# In[188]:


fecha = date.today()
fecha


# In[189]:


# mes actual
año = str(fecha. year) 
mes = str(fecha. month) 
año_mes = año + '-' + mes
año_mes


# In[190]:


# mes previo
mes_previo = (fecha. month) - 1

# if mes enero / cambia el año al previo y mes de 0 a 12
año_previo = (fecha. year)
if mes_previo == 0:
    mes_previo = 12
    año_previo = (fecha. year)-1
año_prev = str(año_previo)
mes_prev = str(mes_previo)
año_mes_prev = año_prev + '-' + mes_prev
año_mes_prev


# In[191]:


# cmg_real mes
link_cmg_real_mes_pasado = 'https://sipubv1.api.coordinador.cl/api/v1/cmg_files/mensuales/' + año_mes_prev + '.tsv?user_key=f3cdad2758436a0a2c2c1fec92853de7'
link_cmg_prog_mes_pasado = 'https://sipubv1.api.coordinador.cl/api/v1/cmg_files/barra/mensuales/' + año_mes_prev + '.tsv?user_key=f3cdad2758436a0a2c2c1fec92853de7'

# cmg_real mes
link_cmg_real = 'https://sipubv1.api.coordinador.cl/api/v1/cmg_files/mensuales/' + año_mes + '.tsv?user_key=f3cdad2758436a0a2c2c1fec92853de7'
link_cmg_prog = 'https://sipubv1.api.coordinador.cl/api/v1/cmg_files/barra/mensuales/' + año_mes + '.tsv?user_key=f3cdad2758436a0a2c2c1fec92853de7'


# In[192]:


from selenium import webdriver


# In[193]:


options = webdriver.ChromeOptions()


# In[194]:


preferences = {"download.default_directory": main_path}


# In[195]:


options.add_experimental_option("prefs", preferences)


# In[196]:


driver= webdriver.Chrome(options=options)


# In[197]:


# cmg_real 
driver.get(link_cmg_real)
time.sleep(20)


# In[198]:


# cmg_real_mes_previo
driver.get(link_cmg_real_mes_pasado)
time.sleep(20)


# In[199]:


# cmg_prog
driver.get(link_cmg_prog)
time.sleep(20)


# In[200]:


# cmg_prog_mes_previo
driver.get(link_cmg_prog_mes_pasado)
time.sleep(20)


# In[201]:


driver.close();


# In[202]:


# rename cmg_real


# In[203]:


# rename cmg_real
old_name_cmg_real = '2022-6.tsv'
new_name_cmg_real = 'cmg_real.tsv'
old_name_cmg_real_mes_previo = '2022-5.tsv'
new_name_cmg_real_mes_previo = 'cmg_real_mes_previo.tsv'

# rename cmg_prog
old_name_cmg_prog = '2022-6 (1).tsv'
new_name_cmg_prog = 'cmg_prog.tsv'
old_name_cmg_prog_mes_previo = '2022-5 (1).tsv'
new_name_cmg_prog_mes_previo = 'cmg_prog_mes_previo.tsv'


# In[204]:


os.rename(old_name_cmg_real, new_name_cmg_real)
os.rename(old_name_cmg_real_mes_previo, new_name_cmg_real_mes_previo)
os.rename(old_name_cmg_prog, new_name_cmg_prog)
os.rename(old_name_cmg_prog_mes_previo, new_name_cmg_prog_mes_previo)


# In[180]:


os.path.join(main_path, new_name_cmg_real)


# In[181]:


new_name_cmg_real


# In[223]:


#new_name_cmg_real
cmg_real = pd.read_csv(os.path.join(main_path, new_name_cmg_real), sep = '\t', decimal = ',')
#new_name_cmg_real_mes_previo
cmg_real_mes_previo  = pd.read_csv(os.path.join(main_path, new_name_cmg_real_mes_previo), sep = '\t', decimal = ',')
#new_name_cmg_prog
cmg_prog  = pd.read_csv(os.path.join(main_path, new_name_cmg_prog), sep = '\t', decimal = ',')
#new_name_cmg_prog_mes_previo
cmg_prog_mes_previo  = pd.read_csv(os.path.join(main_path, new_name_cmg_prog_mes_previo), sep = '\t', decimal = ',')



# In[224]:


cmg_real


# In[225]:


# JUNTAR cmg_real CON cmg_real_mes_previo
frames_cmg = [cmg_real_mes_previo, cmg_real]
result_cmg = pd.concat(frames_cmg, axis = 0)
result_cmg.reset_index(drop=True, inplace=True)
result_cmg


# In[226]:


# format result_cmg
result_cmg['fecha'] = result_cmg['fecha'].astype('datetime64[ns]')
result_cmg['fecha'] = result_cmg['fecha'].astype('datetime64[ns]')
result_cmg['hora'].replace(24, 0,inplace=True)
#result_cmg['fecha_hora'] = result_cmg['fecha'].astype(str) + ' ' + result_cmg['hora'].astype(str)
#result_cmg.drop(['costo_en_pesos'], axis = 1, inplace=True)
result_cmg.tail()


# In[227]:


for i in range (len(result_cmg)):
    if result_cmg['hora'].iloc[i] == 0:
        result_cmg['fecha'].iloc[i] = result_cmg['fecha'].iloc[i] + timedelta(days=1)
        


# In[228]:


result_cmg['hora'] = pd.to_timedelta(result_cmg.hora, unit='h')
result_cmg['hora'] = result_cmg['hora'].astype(str)
result_cmg['hora'] = result_cmg['hora'].str[-8:]
result_cmg['fecha'] = result_cmg['fecha'].astype(str) + ' ' + result_cmg['hora']
result_cmg['fecha'] = pd.to_datetime(result_cmg['fecha'], format='%Y%m%d %H:%M:%S')
result_cmg.tail()


# ## Read CMg Reales BD

# In[2]:


dbuser = 'aaylwin'
pwd = 'WYpbrRE2yWwYBC'
server_ip = '127.0.0.1:3310'
port = '3310'
db_instance = 'gestion_integral'


# In[3]:


import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine


# In[4]:


engine = create_engine('mysql+pymysql://'+dbuser+':'+pwd+'@'+server_ip+'/'+db_instance, echo=False)
conn = engine.connect()
conn


# In[6]:


# cmg real 
#table_name = 'gi_cmg_real'
df_cmg_real = pd.read_sql('SELECT * FROM gi_cmg_real ORDER BY fecha DESC LIMIT 1000;', con=conn)
df_cmg_real = df_cmg_real.sort_values(by=['fecha'])
df_cmg_real


# In[7]:


# agarrar ultima fecha
df_cmg_real['fecha'].tail(1)


# In[238]:


df_cmg_real.dtypes


# In[8]:


# ultima fecha bd
end_bd = str(df_cmg_real['fecha'].tail(1).iloc[0])
end_bd = pd.to_datetime(end_bd, format='%Y%m%d %H:%M:%S')
end_bd


# In[10]:


# primer bloque despues de ultimo registro 
start_append = end_bd - timedelta(days=1)
start_append


# In[44]:


start_append_string = start_append.strftime("%Y-%m-%d %H:%M:%S")
start_append_string


# In[45]:


# crear df a repetir (ultimo dia)
query = "SELECT * FROM gi_cmg_real WHERE fecha  > " + "'" + start_append_string + "'"

df_cmg_loop = pd.read_sql(query, con=conn)

df_cmg_loop


# In[48]:


end_bd


# In[50]:


end_bd.strftime("%Y-%m-%d")


# In[52]:


#calcular dias entre end_bd y dia ayer
cmg_real_last = end_bd.strftime("%Y-%m-%d")
ayer = str(date.today()-timedelta(days=1))
cmg_real_last, ayer
# convert string to date object
d1 = datetime.strptime(cmg_real_last, "%Y-%m-%d")
d2 = datetime.strptime(ayer, "%Y-%m-%d")
# difference between dates in timedelta = numero de loops
delta = d2 - d1
loops = delta.days
loops


# In[53]:


len(df_cmg_loop)


# In[55]:


df_loop = df_cmg_loop


# In[56]:


if loops > 0:
    df = pd.DataFrame()
    for x in range (loops):
        for i in range (len(df_cmg_loop)):
            df_loop['fecha'].iloc[i] = df_loop['fecha'].iloc[i] + timedelta(days=loops)
            frames_df = [df,df_loop]
            result_df = pd.concat(frames_df)
        df = result_df
else:
    df = df_loop.head(0)


# In[57]:


df


# In[58]:


df.dtypes


# df = pd.DataFrame()
# for x in range (loops):
#     for i in range (len(result_cmg_append)):
#         df_loop['fecha'].iloc[i] = df_loop['fecha'].iloc[i] + timedelta(days=loops)
#         frames_df = [df,df_loop]
#         result_df = pd.concat(frames_df)
#     df = result_df

# In[59]:


conn.close()


# ## Insert CMg provisionales

# In[60]:


dbuser = 'aaylwin'
pwd = 'WYpbrRE2yWwYBC'
server_ip = '127.0.0.1:3310'
port = '3310'
db_instance = 'gestion_integral_staging'


# In[61]:


import mysql.connector 
import sqlalchemy
from mysql.connector import Error
from sqlalchemy import create_engine


# In[62]:


engine = create_engine('mysql+pymysql://'+dbuser+':'+pwd+'@'+server_ip+'/'+db_instance, echo=False)
conn = engine.connect()


# In[63]:


conn


# In[66]:


table_name = 'cmg_provisional'
cmg_prov = pd.read_sql('SELECT * FROM cmg_provisional', con=conn)
cmg_prov


# In[67]:


df


# In[68]:


# insert a bd
df.to_sql(con=conn, name = 'cmg_provisional', if_exists = 'replace', index=False)


# In[69]:


conn.close()

