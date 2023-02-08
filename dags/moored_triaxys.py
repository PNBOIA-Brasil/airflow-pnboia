#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
		'owner': 'PNBOIA',
		'start_date': datetime(2022, 3, 4),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
triaxys = DAG('triaxys',
		default_args=default_args,
		description='triaxys Buoy',
		schedule_interval='13 * * * *', 
		catchup=False,
		tags=['moored, triaxys']
)

# python callable function
def get_data_triaxys_raw():
    from remobsqc.buoys.triaxys import Triaxys
    t = Triaxys(data_type='raw')
    t.get(save_db=True)

def get_data_triaxys_status():
    from remobsqc.buoys.triaxys import Triaxys
    t = Triaxys(data_type='status')
    t.get(save_db=True)

def get_data_triaxys():
    from remobsqc.buoys.triaxys import Triaxys
    t = Triaxys(data_type='general')
    t.get(save_db=True)

def qualify_data_triaxys():
    from remobsqc.buoys.triaxys import Triaxys
    t = Triaxys(data_type='general')
    t.qualify_data(save_db=True)

get_raw_data_task = PythonOperator(task_id='get_raw_data', python_callable=get_data_triaxys_raw, dag=triaxys)
get_status_data_task = PythonOperator(task_id='get_status_data', python_callable=get_data_triaxys_status, dag=triaxys)
get_data_task = PythonOperator(task_id='get_general_data', python_callable=get_data_triaxys, dag=triaxys)
qualify_data_task = PythonOperator(task_id='qualify_data', python_callable=qualify_data_triaxys, dag=triaxys)

# Set the order of execution of tasks. 
get_raw_data_task >> get_status_data_task >> get_data_task >> qualify_data_task