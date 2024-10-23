import datetime
import requests,json
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


@dag(
    start_date=datetime.datetime(2024, 8, 31),
    schedule="@daily"
)
def carga_inicial():

    def insertAccount():
        return """INSERT INTO accounts (accountKey, accountName, accountDescription) VALUES (%i, %s, %s)"""

    @task
    def extract_order():
        PageNumber = 1
        res = requests.get( f'https://demodata.grapecity.com/adventureworks/api/v1/salesOrders?PageNumber={PageNumber}&PageSize=100')
    
    @task
    def staged_order(listaOrders):
        for order in listaOrders:
            cursor.execute(insertAccount(), order)

    @task
    def extract_customer():
        PageNumber = 1
        res = requests.get( f'https://demodata.grapecity.com/adventureworks/api/v1/customers?PageNumber=`{PageNumber}`&PageSize=10')