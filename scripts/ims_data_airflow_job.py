from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from spp.ims_connector import ims_connector_airflow
from datetime import datetime


dag = DAG('IMS_Loader', description='IMS Data Loader',
          schedule_interval='*/2 * * * *',
          start_date=datetime(2018, 6, 1), catchup=False)

ims_operator = PythonOperator(task_id='load_data', python_callable=ims_connector_airflow.load_from_ims, dag=dag)
