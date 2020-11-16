from datetime import datetime, timedelta
import prefect
from prefect import Parameter, task, Flow
from prefect.tasks.notifications.email_task import EmailTask
from prefect.schedules import IntervalSchedule
import pandas as pd

retry_delay = timedelta(minutes=1)

@task
def get_data():
    return pd.read_csv('https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv')

@task
def calculate_mean_age(df):
    return df.Age.mean()

@task
def print_mean(m):
    print(f"A média calculada foi {m}")

with Flow("Titanic01") as flow:
    data = get_data()
    med = calculate_mean_age(data)
    p = print_mean(med)


flow.register(project_name='IGTI', idempotency_key=flow.serialized_hash() )
