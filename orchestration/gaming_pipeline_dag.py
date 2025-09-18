"""Airflow DAG for BrainRocket Gaming Data Pipeline."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'gaming_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for gaming transaction data',
    schedule_interval=timedelta(hours=1),  # Run hourly
    catchup=False,
    tags=['gaming', 'etl', 'data-pipeline'],
)


def generate_transaction_data(**kwargs):
    """Generate synthetic transaction data."""
    import subprocess
    import os
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate filename with timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(data_dir, f'transactions_{timestamp}.csv')
    
    # Run data generator
    cmd = [
        'python', 
        os.path.join(os.path.dirname(__file__), '..', 'etl', 'data_generator.py'),
        '--num-records', '500',  # Generate 500 records per hour
        '--output', output_file
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Data generation failed: {result.stderr}")
    
    # Push the output file path to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='transaction_file', value=output_file)
    
    return output_file


def run_etl_pipeline(**kwargs):
    """Run the ETL pipeline."""
    import subprocess
    import os
    
    # Get the transaction file from XCom
    ti = kwargs['ti']
    transaction_file = ti.xcom_pull(task_ids='generate_data', key='transaction_file')
    
    if not transaction_file:
        raise Exception("No transaction file found from previous task")
    
    # Run ETL pipeline
    cmd = [
        'python', 
        os.path.join(os.path.dirname(__file__), '..', 'etl', 'etl_batch.py'),
        '--input', transaction_file
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"ETL pipeline failed: {result.stderr}")
    
    return f"ETL completed for {transaction_file}"


def refresh_materialized_views(**kwargs):
    """Refresh materialized views."""
    import subprocess
    import os
    
    # This would typically be done within the ETL process,
    # but we include it as a separate task for demonstration
    print("Refreshing materialized views...")
    # In a real scenario, this would connect to the database and refresh views
    return "Materialized views refreshed"


# Define tasks
generate_data_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_transaction_data,
    provide_context=True,
    dag=dag,
)

run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl_pipeline,
    provide_context=True,
    dag=dag,
)

refresh_views_task = PythonOperator(
    task_id='refresh_views',
    python_callable=refresh_materialized_views,
    provide_context=True,
    dag=dag,
)

# Optional: Add a task to clean up old files
cleanup_task = BashOperator(
    task_id='cleanup_old_files',
    bash_command='find {{ params.data_dir }} -name "transactions_*.csv" -mtime +7 -delete',
    params={'data_dir': os.path.join(os.path.dirname(__file__), '..', 'data')},
    dag=dag,
)

# Set task dependencies
generate_data_task >> run_etl_task >> refresh_views_task >> cleanup_task

# For Airflow 2.0+ compatibility
if __name__ == "__main__":
    dag.test()
