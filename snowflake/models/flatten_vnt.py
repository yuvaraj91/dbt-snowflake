import base64
from snowflake.snowpark.functions import any_value    
    
def model(dbt, session):
    """
    """
    dbt.config(
        materialized='table'
        #unique_key='id',
        #incremental_strategy='merge'
        )
    #delta_hours = dbt.config.get("delta_hours")
    #timestamp_column = dbt.config.get("timestamp_column")
    df = session.table("public.vnt")
    df = df.join_table_function("flatten", df["SRC"]).drop(["SRC", "SEQ", "PATH", "INDEX", "THIS"])
    cols = [c[0] for c in df.group_by("key").agg(any_value("key")).collect()]
    df = df.pivot("key", cols).min("value")
    renamed_columns = list(map(lambda x: x.replace('"\'', '').replace('\'"',''), df.columns))
    df = df.toDF(*renamed_columns)
        
    return df
