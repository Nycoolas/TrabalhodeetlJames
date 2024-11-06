# mydag.py
import datetime
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from etl_carga import load_data_from_zip  # Importa a função para carregar os dados do ZIP

DLL_TABLE = """
CREATE TABLE IF NOT EXISTS accounts (
    accountDescription varchar(200),
    accountName varchar(200),
    accountKey int
);
"""

def insertAccount(accountKey, accountName, accountDescription):
    return f"""
    INSERT INTO accounts (accountKey, accountName, accountDescription) 
    VALUES ({accountKey}, '{accountName}', '{accountDescription}')
    """

@dag(
    start_date=datetime.datetime(2024, 8, 31),
    schedule="@daily"
)
def generate_dag():
    
    @task
    def extract_zip_data():
        zip_folder = "/Applications/trabalhodeetl20242/data"
        extract_folder = "/Applications/trabalhodeetl20242/tmp"
        load_data_from_zip(zip_folder, extract_folder)
        return "Dados extraídos e processados dos arquivos ZIP"

    @task
    def extract_api_data():
        pageNumber = 1
        res = requests.get(f'https://demodata.grapecity.com/contoso/api/v1/accounts?PageNumber={pageNumber}&PageSize=100')
        return res.text

    @task
    def transformation(result):
        transformed_data = []
        for record in json.loads(result):
            accountDescription = record['accountDescription']
            accountName = record['accountName']
            accountKey = record['accountKey']
            transformed_data.append((accountKey, accountName, accountDescription))
        return transformed_data

    @task
    def load(records):
        pg_hook = PostgresHook('dw_stage')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(DLL_TABLE)
        conn.commit()
        for record in records:
            cursor.execute(insertAccount(record[0], record[1], record[2]))
        conn.commit()
        cursor.close()
        conn.close()

    # Definindo a execução das tarefas
    zip_data = extract_zip_data()
    api_data = extract_api_data()
    transformed_data = transformation(api_data)
    load(transformed_data)

generate_dag()
