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
spotterdrift = DAG('spotterdrift',
		default_args=default_args,
		description='spotter drift Buoy',
		schedule_interval='09 * * * *', 
		catchup=False,
		tags=['drift, spotter']
)

# python callable function
def get_data_spotter_status():
    from remobsqc.buoys.spotter import Spotter
    p = Spotter(data_type='general', buoy_type='drift')
    p.get(save_db=True)

def get_data_spotter():
    from remobsqc.buoys.spotter import Spotter
    p = Spotter(data_type='status', buoy_type='drift')
    p.get()

def get_freq_spotter():
    from remobsqc.buoys.spotter import Spotter
    p = Spotter(data_type='freq', buoy_type='drift')
    p.get(save_db=True)

get_status_data_task = PythonOperator(task_id='get_status_data', python_callable=get_data_spotter_status, dag=spotterdrift)
get_data_task = PythonOperator(task_id='get_data', python_callable=get_data_spotter_status, dag=spotterdrift)
get_freq_data_task = PythonOperator(task_id='get_freq_data', python_callable=get_freq_spotter, dag=spotterdrift)

# Set the order of execution of tasks. 
get_status_data_task >> get_data_task >> get_freq_data_task