from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from spp.data_connector import load_data
from datetime import datetime


from spp.utils import get_data_connector_parameters

params = get_data_connector_parameters()

schedule_interval ='*/%d * * * *' % int(params['period'] / 60)
dag = DAG('IMS_Loader', schedule_interval=schedule_interval,
          start_date=datetime(2018, 6, 1), **params['airflow'])

ims_operator = PythonOperator(task_id='load_data', python_callable=load_data,
                              dag=dag)
