import base64
import pandas as pd

def encoding(x):
    x = x.encode("ascii")
    base64_bytes = base64.b64encode(x)
    return base64_bytes.decode("ascii")
    
    
def model(dbt, session):
    dbt.config(materialized="table")
    
    df = dbt.source('public', 'addresses').to_pandas()
    df['STREET'] = df['STREET'].apply(lambda x: encoding(x) if pd.notnull(x) else x)

    return df
