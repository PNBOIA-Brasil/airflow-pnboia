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
criosfera = DAG('criosfera',
		default_args=default_args,
		description='Criosfera Buoy',
		schedule_interval='06 * * * *', 
		catchup=False,
		tags=['moored, criosfera']
)

# python callable function
def get_data_criosfera():
    from remobsqc.buoys.hidromares import Hidromares
    h = Hidromares()
    h.get(save_db=True)

def qualify_data_criosfera():
    from remobsqc.buoys.hidromares import Hidromares
    h = Hidromares()
    h.qualify_data(save_db=True)

get_data_task = PythonOperator(task_id='get_data', python_callable=get_data_criosfera, dag=criosfera)
qualify_data_task = PythonOperator(task_id='qualify_data', python_callable=qualify_data_criosfera, dag=criosfera)


get_data_task >> qualify_data_task
