#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
		'owner': 'PNBOIA',
		'start_date': datetime(2022, 3, 4),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
spotter = DAG('spotter',
		default_args=default_args,
		description='spotter Buoy',
		schedule_interval='10 * * * *', 
		catchup=False,
		tags=['moored, spotter']
)

# python callable function
def get_data_spotter_status():
    from remobsqc.buoys.spotter import Spotter
    p = Spotter(data_type='status', buoy_type='moored')
    p.get(save_db=True)

def get_data_spotter():
    from remobsqc.buoys.spotter import Spotter
    p = Spotter(data_type='all', buoy_type='moored')
    p.get(save_db=True)

def qualify_data_spotter():
    from remobsqc.buoys.spotter import Spotter
    p = Spotter(data_type='all', buoy_type='moored', load_api=False)
    p.qualify_data(save_db=True)


get_status_data_task = PythonOperator(task_id='get_status_data', python_callable=get_data_spotter_status, dag=spotter)
get_data_task = PythonOperator(task_id='get_data', python_callable=get_data_spotter_status, dag=spotter)
qualify_data_task = PythonOperator(task_id='qualify_data', python_callable=qualify_data_spotter, dag=spotter)

# Set the order of execution of tasks. 
get_status_data_task >> get_data_task >> qualify_data_task