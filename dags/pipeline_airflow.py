from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'api_data_pipeline',
    default_args=default_args,
    description='Pipeline para leitura, transformação e agregação de dados da API',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    

    # Defina as tarefas
    inicio = DummyOperator(
        task_id='inicio',
        dag=dag,
    )

    task_leitura_api = BashOperator(
        task_id='leitura_api',
        bash_command='python /opt/airflow/dags/scripts/leituraApi.py',
    )

    task_transformacao = BashOperator(
        task_id='transformacao',
        bash_command='python /opt/airflow/dags/scripts/transformacao.py',
    )

    task_agregacao = BashOperator(
        task_id='agregacao',
        bash_command='python /opt/airflow/dags/scripts/tabelaAgregada.py',
    )

    fim = DummyOperator(
        task_id='fim',
        dag=dag,
    )


    inicio >> task_leitura_api >> task_transformacao >> task_agregacao >> fim
