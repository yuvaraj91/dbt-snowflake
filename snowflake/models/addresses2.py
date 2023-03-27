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
        incremental_strategy='merge'
        )   

    df = dbt.source('public', 'addresses')
    
    if dbt.is_incremental:
        updated_at = f"select max(_airbyte_emitted_at) - INTERVAL '3 DAY' from public.addresses;"

        df = df.filter(df.created >= session.sql(updated_at).collect()[0][0]).to_pandas()
        
        #df['CREATED'] = df['CREATED'].astype(str)
        #df['MODIFIED'] = df['MODIFIED'].astype(str)
        df['CREATED'] = df['CREATED'].dt.tz_localize("UTC+00:00").dt.ceil(freq='ms')
        df['MODIFIED'] = df['MODIFIED'].dt.tz_localize("UTC+00:00").dt.ceil(freq='ms')
        
        #df['_AIRBYTE_ADDRESSES_HASHID'] = df['_AIRBYTE_ADDRESSES_HASHID'].str[:32]
        #df['_AIRBYTE_UNIQUE_KEY'] = df['_AIRBYTE_UNIQUE_KEY'].str[:32]
        
    #df = df.filter(df._airbyte_normalized_at >= pd.Timestamp.now() - pd.to_timedelta("1day"))
    #df = df.filter(df._airbyte_normalized_at >= F.dateadd("day", F.lit(-3), F.current_timestamp()))
    

        df['STREET'] = df['STREET'].apply(lambda x: encoding(x) if pd.notnull(x) else x)
        
    return df
