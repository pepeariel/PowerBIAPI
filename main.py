from databricks import sql
import pandas as pd
import numpy as np
import json
import time
import requests
from queries import query_pedidos_abertos
import os
from dotenv import load_dotenv
import warnings
import datetime
warnings.filterwarnings("ignore")

"""
This code does:

1 - Connect to Datbricks SQL endpoint
2 - Send a query to a ouro database, retrieve its data and transform it to pandas and json format
3 - Send the json data to a push Power BI dataset (streaming)

References to Power BI API methods used, can be found here:

https://learn.microsoft.com/en-us/rest/api/power-bi/push-datasets/datasets-post-rows#code-try-0

"""

# Load local variables
load_dotenv()
token = os.getenv('token') # Power BI API token
access_token = os.getenv('access_token') # DataBricks Token

# Create a connection to databricks SQL endpoint, and given a sql query, return a dataframe
def CreateDataLakeCon(query, access_token):
    with sql.connect(server_hostname = "adb-1220993250695902.2.azuredatabricks.net",
                     http_path       = "/sql/1.0/warehouses/6b94e3b6ef796336",
                     access_token    =  access_token) as con:
        with con.cursor() as cursor:
            df = pd.read_sql(query, con) # Read sql query in DataFrame format
    return df

def LogLastSuccessPush(file_path, linhas):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(file_path, 'w') as file:
        file.write(f"Linhas inseridas: {linhas} - Date: {current_time}")
        
# Create dataframe based on a SQL query made to databricks
df_push = CreateDataLakeCon(query_pedidos_abertos, access_token)

# Pipeline to send data correctly to streaming dataset
df_push['data_integracao'] = df_push['data_integracao'].astype(str)
df_push['inicioseparacao'] = df_push['inicioseparacao'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('01-01-1999 00:00:00')
df_push['fimseparacao'] = df_push['fimseparacao'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('01-01-1999 00:00:00')
df_push['inicioconferencia'] = df_push['inicioconferencia'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('01-01-1999 00:00:00')
df_push['fimconferencia'] = df_push['fimconferencia'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('01-01-1999 00:00:00')
df_push['dt_embarque'] = df_push['dt_embarque'].astype(str)
df_push['data_expedicao'] = df_push['data_expedicao'].astype(str) 
# Unica coluna que precisa estar em formato de data
df_push['ingestao_utc_saoPaulo'] = df_push['ingestao_utc_saoPaulo'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Transform pandas dataframe to json format
json_data = df_push.to_dict('records')

# Delete Data
# Credencials - hide later!
delete_url = "https://api.powerbi.com/v1.0/myorg/datasets/23cdd283-ecf1-4502-8929-43a56207a35a/tables/RealTimeData/rows"
headers = {"Authorization": f"Bearer {token}"}

# Drop the Streaming data
response_delete = requests.delete(delete_url, headers = headers,verify=False)
if response_delete.status_code == 200: 
    print('Rows deleted successfully!')

time.sleep(30)

# Send data to Streaming dataset
try:
    url_push = "https://api.powerbi.com/beta/e4ac844a-b483-48f2-a621-4e50119811d7/datasets/23cdd283-ecf1-4502-8929-43a56207a35a/rows?experience=power-bi&key=ZRbihDkMKxmzBydgF76e%2BOxpjxitSuQKK8x2RYeqnZUabV%2FTiiXNGx5gc1n3op3b6uitcmTOSg%2FUuYqKXuP%2BRA%3D%3D"
    response_push = requests.post(url_push, data=json.dumps(json_data), verify=False)
    if response_push.status_code == 200:
        linhas = len(json_data)
        print("Rows inserted: ", linhas)
        LogLastSuccessPush("output.txt", linhas)

except Exception as e:
    print('Not possible to send data, error: ', e)