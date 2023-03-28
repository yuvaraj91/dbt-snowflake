import base64
import pandas as pd

def encoding(x):
    x = x.encode('ascii', 'ignore')
    base64_bytes = base64.b64encode(x)
    return base64_bytes.decode('ascii')
    
    
def model(dbt, session):
    dbt.config(
        materialized='incremental',
        unique_key='photographer_id',
        incremental_strategy='merge',
        merge_update_columns= ['STREET']
        )   
    df = dbt.source('public', 'addresses')
    
    if dbt.is_incremental:
        updated_at = f"select max(_airbyte_emitted_at) - INTERVAL '3 DAY' from public.addresses;"
        df = df.filter(df.created >= session.sql(updated_at).collect()[0][0]).to_pandas()
        df['CREATED'] = df['CREATED'].dt.tz_localize('UTC')
        df['MODIFIED'] = df['MODIFIED'].dt.tz_localize('UTC')
        df['STREET'] = df['STREET'].apply(lambda x: encoding(x) if pd.notnull(x) else x)
    return df
