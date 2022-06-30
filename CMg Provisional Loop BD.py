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

