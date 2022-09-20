#!/usr/bin/python3

from datetime import datetime
import os
import sqlparse
import pandas as pd
import connection

def query(file):
    path = open(os.getcwd()+'/query/'+file, 'r').read()
    query = sqlparse.format(path, strip_comments=True).strip()
    return query

if __name__ == '__main__':
    
    df = pd.read_sql(query('dim_orders.sql'), connection.database())
    df.to_sql('dim_orders', connection.warehouse(), if_exists='replace', index=False)

    filetime = datetime.now().strftime('%Y%m%d')

    path = os.getcwd()+'/local/'
    if not os.path.exists(path):
        os.makedirs(path)
    df.to_csv(f'{path}dim_orders_{filetime}.csv', index=False)

    client = connection.hadoop()
    client.write(f'/dim_orders_{filetime}.csv', data=df.to_csv(index=False))