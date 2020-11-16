from datetime import datetime, timedelta
import prefect
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import pandas as pd

retry_delay = timedelta(minutes=1)
schedule = IntervalSchedule(interval=timedelta(minutes=2))

@task
def get_data():
    df = pd.read_csv("https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv")
    return df

@task
def calcula_media_idade(df):
    return df.Age.mean()

@task
def exibe_media_calculada(m):
    logger = prefect.context.get("logger")
    logger.info(f"A média de idade calculada foi {m}")

@task
def exibe_dataset(df):
    logger = prefect.context.get("logger")
    logger.info(df.head(3).to_json())

with Flow("Titanic01", schedule=schedule) as flow:
    df = get_data()
    med = calcula_media_idade(df)
    e = exibe_media_calculada(med)
    ed = exibe_dataset(df)

flow.register(project_name='IGTI', idempotency_key=flow.serialized_hash() )
flow.run_agent(token="0dZVAbOVJZ6ZZawMdmocFQ")