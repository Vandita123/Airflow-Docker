from sqlalchemy import create_engine
import os

def load(df):
    POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
    POSTGRES_PWD = os.getenv("POSTGRES_PASSWORD", "airflow")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")

    db_uri = f"postgresql://{POSTGRES_USER}:{POSTGRES_PWD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    engine = create_engine(db_uri)
    
    df.to_sql('random_users', engine, if_exists='replace', index=False)

"""def load(df):
    df.to_csv('/opt/airflow/data/transformed_sales_data.csv',index=False)"""