import requests
import pandas as pd

def extract():
    response = requests.get("https://randomuser.me/api/?results=10")
    data = response.json()
    records = data['results']

    df = pd.json_normalize(records)
    return df

"""def extract():
    df = pd.read_csv('/opt/airflow/data/sales_data.csv')
    return df"""