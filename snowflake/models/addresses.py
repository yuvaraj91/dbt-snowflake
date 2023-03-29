import base64
from snowflake.snowpark.functions import md5    
    
def model(dbt, session):
    dbt.config(
        materialized='incremental',
        unique_key='photographer_id',
        incremental_strategy='merge',
        merge_update_columns= ['STREET']
        )
    delta_hours = dbt.config.get("delta_hours", '1 DAY')   
    df = dbt.source('public', 'addresses')
    
    if dbt.is_incremental:
        updated_at = f"select max(_airbyte_emitted_at) - INTERVAL '{delta_hours}' from {dbt.this};"
        df = df.filter(df.created >= session.sql(updated_at).collect()[0][0])
        df = df.with_column("STREET", md5("STREET"))
        
    return df
