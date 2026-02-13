import os
import pandas as pd
from google.cloud import bigquery
from logger import get_logger
logger=get_logger("load")


def load_to_bigquery(clean_path,project_id,dataset_id,table_id,write_mode="WRITE_APPEND"):
    cred=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS env var is not set!")
    
    df=pd.read_csv(clean_path)
    client=bigquery.Client(project=project_id)
    table_ref=f"{project_id}.{dataset_id}.{table_id}"
    df["date"]=pd.to_datetime(df["date"],errors="coerce")
    last_date=get_last_date(client,table_ref)
    if last_date is not None:
        df=df[df["date"]>last_date]
    if df.empty:
        logger.info(f"No new rows to load. Bigquery last date={last_date}")
        return
  

    job_config=bigquery.LoadJobConfig(write_disposition=write_mode)
    job=client.load_table_from_dataframe(df,table_ref,job_config=job_config)
    job.result()
    logger.info(f"Loaded {len(df)} rows into {table_ref}")

def get_last_date(client,table_id):
    query=f"""
    SELECT MAX(date) as last_date
    FROM `{table_id}`
    """
    try:
        result=client.query(query).to_dataframe()
        return result["last_date"].iloc[0]
    except Exception:
        return None
