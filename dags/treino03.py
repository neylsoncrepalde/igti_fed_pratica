from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile

default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": datetime(2020, 11, 14, 23, 50),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False
    #"retries": 1,
    #"retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "treino-03", 
    description="Uma dag para processamento de um dado maior",
    default_args=default_args, 
    schedule_interval=None
)


get_data = BashOperator(
    task_id="get-data",
    bash_command='curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip',
    trigger_rule="all_done",
    dag=dag
)


def unzip_file():
    with zipfile.ZipFile("/usr/local/airflow/data/microdados_enade_2019.zip", 'r') as zipped:
        zipped.extractall("/usr/local/airflow/data")

unzip_data = PythonOperator(
    task_id='unzip-data',
    python_callable=unzip_file,
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('/usr/local/airflow/data/microdados_enade_2019/2019/3.DADOS/microdados_enade_2019.txt', sep=';', decimal=',')
    print("Estas são as colunas do dataset Enade:")
    print(df.columns)
    print('Calculando a idade média dos alunos:')
    med = df.NU_IDADE.mean()
    return med

task_calculate_mean = PythonOperator(
    task_id='calculate-mean-age',
    python_callable=calculate_mean_age,
    dag=dag
)

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calculate-mean-age')
    print(f"Idade média dos alunos no Enade 2019 foi {value}")

task_mean_age = PythonOperator(
    task_id="say-mean-age",
    python_callable=print_age,
    provide_context=True,
    dag=dag
)

def print_age_sq(**context):
    value = context['task_instance'].xcom_pull(task_ids='calculate-mean-age')
    print(f"Idade média ao quadrado {value**2}")

task_age_sq = PythonOperator(
    task_id="say-age-squared",
    python_callable=print_age_sq,
    provide_context=True,
    dag=dag
)

get_data >> unzip_data >> task_calculate_mean >> [task_mean_age, task_age_sq]