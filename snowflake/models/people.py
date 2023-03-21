import base64
import pandas

def encoding(x):
    x = x.encode("ascii")
    base64_bytes = base64.b64encode(x)
    return base64_bytes.decode("ascii")
    
    
def model(dbt, session):
    dbt.config(materialized="table")
    
    df = dbt.source('public', 'people').to_pandas()
    df['FAMILY_NAME'] = df['FAMILY_NAME'].apply(encoding)

    return df
