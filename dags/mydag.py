# https://medium.com/@abhijitgunjal1648/building-a-data-pipeline-with-apache-airflow-and-postgresql-74bfeeab6455
import datetime
import requests,json
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

DLL_TABLE = "CREATE TABLE IF NOT EXISTS accounts (accountDescription varchar(200), accountName varchar(200), accountKey int)"

def insertAccount(accountKey, accountName, accountDescription):
    return f"""INSERT INTO accounts (accountKey, accountName, accountDescription) VALUES ({accountKey}, '{accountName}', '{accountDescription}')"""

@dag(
    start_date=datetime.datetime(2024, 8, 31),
    schedule="@daily"
)
def generate_dag():

    @task
    def extract():
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
            transformed_data.append((accountKey,accountName,accountDescription) )
        return transformed_data
    
    @task
    def load(records):
        pg_hook = PostgresHook('dw_stage')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(DLL_TABLE)
        conn.commit()
        for record in records:
            print(record)
            cursor.execute(insertAccount(record[0],record[1],record[2] )) # , , accountDescription
        
        conn.commit()
        cursor.close()
        conn.close()


    @task
    def load2(result):
        return f"Hello dog!"
    @task
    def final():
        return f"Hello dog!"
    
    extract_operator = extract()
    tranformation_operator = transformation(extract_operator)
    load_operator = load(tranformation_operator)
    load2_operator = load2(tranformation_operator)
    final_operator = final()

    extract_operator >> tranformation_operator >> load_operator >> final_operator

generate_dag()
