from prefect import Flow, task
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@task
def extract():
    return [1, 2, 3]


@task
def transform(x):
    return [i * 10 for i in x]


@task
def load(y):
    logger.info("Received y: {}".format(y))


with Flow("ETL") as flow:
    e = extract()
    t = transform(e)
    l = load(t)

flow.register(project_name='Hello, World!', idempotency_key=flow.serialized_hash() )
#flow.run_agent()