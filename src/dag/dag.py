import sys
sys.path.insert(0, "/opt/airflow/dags/dag/tasks")

from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from input_golden import process_and_input_on_golden
from input_basepq import input_on_silver_basepq
from input_base_cliente import input_on_silver_base_cliente
from input_base_nivel import input_on_silver_base_nivel
from input_base_funcionarios import input_on_silver_base_funcionarios
from input_base_cep import input_on_silver_base_cep
from input_base_cargos import input_on_silver_base_cargos



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'silver_to_golden',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)


def my_test_function():
    return 'The test function has run successfully.'


task0 = PythonOperator(
    task_id='start',
    python_callable=my_test_function,
    dag=dag,
)

task1 = PythonOperator(
    task_id="input-silver-base-cargos",
    python_callable=input_on_silver_base_cargos, dag=dag
)

task2 = PythonOperator(
    task_id="input-silver-base-cep",
    python_callable=input_on_silver_base_cep, dag=dag
)


task3 = PythonOperator(
    task_id="input-silver-base_funcionarios",
    python_callable=input_on_silver_base_funcionarios, dag=dag
)


task4 = PythonOperator(
    task_id="input-silver-base_nivel",
    python_callable=input_on_silver_base_nivel, dag=dag
)


task5 = PythonOperator(
    task_id="input-silver-base_cliente",
    python_callable=input_on_silver_base_cliente, dag=dag
)


task6 = PythonOperator(
    task_id="input-silver-basepq",
    python_callable=input_on_silver_basepq, dag=dag
)


task7 = PythonOperator(
    task_id="process_and_input_on_golden",
    python_callable=process_and_input_on_golden, dag=dag
)


task0 >> [task1, task2, task3, task4, task5, task6] >> task7
