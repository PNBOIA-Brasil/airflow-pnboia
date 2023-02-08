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
bmobr = DAG('bmobr',
		default_args=default_args,
		description='bmobr Buoy',
		schedule_interval='03 * * * *', 
		catchup=False,
		tags=['moored, bmobr']
)

# python callable function
def get_data_bmobr_raw():
    from remobsqc.buoys.bmo_br import BmoBr
    b = BmoBr(data_type='raw')
    b.get(save_db=True)

def get_data_bmobr():
    from remobsqc.buoys.bmo_br import BmoBr
    b = BmoBr(data_type='general')
    b.get(save_db=True)

def qualify_data_bmobr():
    from remobsqc.buoys.bmo_br import BmoBr
    b = BmoBr(data_type='general')
    b.qualify_data(save_db=True)


get_raw_data_task = PythonOperator(task_id='get_raw_data', python_callable=get_data_bmobr_raw, dag=bmobr)
get_data_task = PythonOperator(task_id='get_general_data', python_callable=get_data_bmobr, dag=bmobr)
qualify_data_task = PythonOperator(task_id='qualify_data', python_callable=qualify_data_bmobr, dag=bmobr)

# Set the order of execution of tasks.
get_raw_data_task >> get_data_task >> qualify_data_task