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
alerts = DAG('alerts',
		default_args=default_args,
		description='alert system',
		schedule_interval='25 * * * *', 
		catchup=False,
		tags=['alerts']
)

# python callable function
def position():
    from remobsqc.alerts.moored import MooredAlert
    m = MooredAlert()
    m.position()

def transmission():
    from remobsqc.alerts.moored import MooredAlert
    m = MooredAlert()
    m.transmission()

def transmission_gap():
    from remobsqc.alerts.moored import MooredAlert
    m = MooredAlert()
    m.transmission_gap()

def high_values():
    from remobsqc.alerts.moored import MooredAlert
    m = MooredAlert()
    m.high_values()

def low_values():
    from remobsqc.alerts.moored import MooredAlert
    m = MooredAlert()
    m.low_values()


position_task = PythonOperator(task_id='position', python_callable=position, dag=alerts)
transmission_task = PythonOperator(task_id='transmission', python_callable=transmission, dag=alerts)
transmission_gap_task = PythonOperator(task_id='transmission_gap', python_callable=transmission_gap, dag=alerts)
high_values_task = PythonOperator(task_id='high_values', python_callable=high_values, dag=alerts)
low_values_task = PythonOperator(task_id='low_values', python_callable=low_values, dag=alerts)


# Set the order of execution of tasks.
(position_task, transmission_task, transmission_gap_task, high_values_task, low_values_task)