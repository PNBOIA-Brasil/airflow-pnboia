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
bmobr_triaxys = DAG('bmobr_triaxys',
		default_args=default_args,
		description='bmobr triaxys Buoy',
		schedule_interval='03 * * * *', 
		catchup=False,
		tags=['moored, bmobr']
)

# python callable function
def get_data_bmobr_triaxys_raw():
    from remobsqc.buoys.bmo_br import BmoBr
    b = BmoBr(data_type='triaxys_raw')
    b.get(save_db=True)

def get_data_bmobr_triaxys():
    from remobsqc.buoys.bmo_br import BmoBr
    b = BmoBr(data_type='triaxys_general')
    b.get(save_db=True)

get_raw_triaxys_data_task = PythonOperator(task_id='get_raw_triaxys_data', python_callable=get_data_bmobr_triaxys_raw, dag=bmobr_triaxys)
get_data_triaxys_task = PythonOperator(task_id='get_triaxys_data', python_callable=get_data_bmobr_triaxys, dag=bmobr_triaxys)

# Set the order of execution of tasks.
get_raw_triaxys_data_task >> get_data_triaxys_task