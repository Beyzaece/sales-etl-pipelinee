from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sys
sys.path.append("/opt/project/scripts")
from transform import transform_data
from load_bigquery import load_to_bigquery
from notifier import task_fail_alert
raw_path="/opt/project/data/Walmart_Sales.csv"
clean_path="/opt/project/data/clean_Walmart_Sales.csv"

def run_extract():
    print(f"Extract OK:{raw_path}")

def run_transform():
    

    df=pd.read_csv(raw_path)
    clean_df=transform_data(df)
    clean_df.to_csv(clean_path,index=False)
    print(f"Transform OK:saved{len(clean_df)} rows to {clean_path}")

def run_quality():
    df=pd.read_csv(clean_path)
    bad_date=df["date"].isna().sum()
    bad_sales=(df["weekly_sales"]<0).sum()
    bad_store=df["store"].isna().sum()

    if bad_date>0:
        raise ValueError(f"Quality Failed :date null rows={bad_date}")
    if bad_sales>0:
        raise ValueError(f"Quality Failed: negative sales rows={bad_sales}")
    if bad_store>0:
        raise ValueError(f"Qaulity Failed:store null rows={bad_store}")
    print("Quality Passed ")

def run_load():
    load_to_bigquery(
        clean_path="/opt/project/data/clean_Walmart_Sales.csv",
        project_id="sales-etl-project-487116",
        dataset_id="sales_data",
        table_id="walmart_sales",
        write_mode="WRITE_TRUNCATE"
    )

with DAG(
    dag_id="sales_etl_pipeline",
    default_args={
        "on_failure_callback":task_fail_alert
    },
    start_date=datetime(2025,2,12),
    schedule="@daily",
    catchup=False,
    tags=["etl"]) as dag:

    quality_task=PythonOperator(
        task_id="quality_task",
        python_callable=run_quality
    )

    extract_task=PythonOperator(
        task_id="extract_task",
        python_callable=run_extract
    )


    transform_task=PythonOperator(
        task_id="transform_task",
        python_callable=run_transform
    )

    load_task=PythonOperator(
        task_id="load_to_bigquery",
        python_callable=run_load
    )


    extract_task >> transform_task >> quality_task >>load_task