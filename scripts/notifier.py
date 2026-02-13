def task_fail_alert(context):
    dag_id=context.get("dag").dag_id
    task_id=context.get("task_instance").task_id
    execution_date=context.get("execution_date")
    print("Pipeline Failed!")
    print(f"DAG:{dag_id}")
    print(f"TASK:{task_id}")
    print(f"DATE:{execution_date}")