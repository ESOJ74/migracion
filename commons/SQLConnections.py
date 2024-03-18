import pyodbc
import pandas as pd
import warnings

def connections():
    conn_str = 'DRIVER={SQL Server};SERVER=frvprosql02.database.windows.net;DATABASE=FRV_ELLIOT_PRD;UID=elliot_prd;PWD=Fotow@tio01'
    return pyodbc.connect(conn_str)

def get_df(query):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        conn=connections()
   
        """cursor = conn.cursor()
        cursor.execute('select * from am.assets_definition')
        rows = cursor.fetchall()
        for row in rows:
            print(row)"""
        return pd.read_sql(query, conn)






