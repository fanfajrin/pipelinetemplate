from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'isfan',
    'start_date': datetime(2023, 9, 7),
    'catchup': False,
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
        'Sequential_Master_pipeline', 
        default_args=default_args, 
        schedule_interval="5 0 * * *",
        tags = ["sequential","pipeline"],
        catchup=False,
    )

#fill with your dags name
dag_list = [
    "first_pipeline",
    "second_pipeline",
    "third_pipeline",
    "and_so_on_pipeline"
]

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)


task_list = []
for dag_name in dag_list:
    trigger_task = TriggerDagRunOperator(
        task_id=f'trigger_{dag_name}',
        trigger_dag_id=f"{dag_name}",
        wait_for_completion=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE 
    )
    
    task_list.append(trigger_task)


index = 0
prev_task = ''

for task in task_list:
    if index == 0:
        start >> task
    else:
        task.set_upstream(prev_task)
        
    if index == len(task_list) - 1:
        end << task
    
    index += 1
    prev_task = task
