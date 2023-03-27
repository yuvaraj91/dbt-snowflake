import base64
import pandas as pd

def encoding(x):
    x = x.encode('ascii', 'ignore')
    base64_bytes = base64.b64encode(x)
    return base64_bytes.decode('ascii')
    
    
def model(dbt, session):
    dbt.config(materialized='incremental')
    df = dbt.source('public', 'addresses')
    max_from_this = f"select max(updated_at) from public."
    df = df.filter(df.updated_at >= session.sql(max_from_this).collect()[0][0])
    
    #df = dbt.source('public', 'addresses').to_pandas()
    #df['STREET'] = df['STREET'].apply(lambda x: encoding(x) if pd.notnull(x) else x)

    return df
