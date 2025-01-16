'''
=================================================
Milestone 3

Name  : Ryan Dionenatan
Batch : FTDS-038-RMT

This DAG is made to automate ETL Process from PostgreSQL to Elasticsearch. The dataset that's being used is a car sale dataset from various US dealer from 2022-2023.
=================================================
'''

# Import Libraries
import pandas as pd
from datetime import datetime
import psycopg2 as pg
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from airflow import DAG
from airflow.decorators import task
import warnings
warnings.filterwarnings('ignore')

# Initiate Start date
default_args= {
    'owner': 'Ryan',
    'start_date': datetime(2024, 11, 1)
}

# Initiate DAG name, description, and cron time
with DAG(
    'etl_dealer_data',
    description='Taking data from Postgres server, clean the data, and load to Elasticsearch',
    schedule_interval='10,20,30 9 * * SAT',
    default_args=default_args, 
    catchup=False) as dag:

    @task()
    def read_from_postgre():
        '''
        This function is for extracting data from PostgreSQL and store raw data into CSV for data cleaning.
        This function takes no parameters.

        Usage example:

        read_from_postgre()
        '''
        # Connect to PostgreSQL
        engine = pg.connect("dbname='airflow' user='airflow' host='postgres' port='5432' password='airflow'")
        # Read from SQL
        df = pd.read_sql('select * from table_m3', con=engine)
        print(df.head())
        # Store Raw Data to CSV
        df.to_csv('/opt/airflow/dags/P2M3_ryan_dionenatan_data_raw.csv', index=False)
    
    @task()
    def preprocess_data():
        '''
        This function is for cleaning the raw CSV file made from read_from_postgre() and save the clean data into CSV for export to Elasticsearch.
        This function takes no parameters.

        Usage example:

        preprocess_data()
        '''
        # Read the Raw Data CSV
        df = pd.read_csv('/opt/airflow/dags/P2M3_ryan_dionenatan_data_raw.csv')

        # Remove whitespace from all column name
        df.columns = df.columns.str.strip()

        # Rename Price ($) column name to values
        df.rename(columns={'Price ($)': 'price'}, inplace=True)

        # Lower case all column name
        df.columns = df.columns.str.lower()

        # Replace space with underscore on column name
        df.columns = df.columns.str.replace(' ', '_')

        # Handle missing values
        df.fillna(df.mean(numeric_only=True), inplace=True)

        # Convert date data type
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Remove duplicates if any
        df.drop_duplicates(inplace=True)

        # Change "DoubleÂ Overhead Camshaft" to "Double Overhead Camshaft"
        df['engine'] = df['engine'].str.encode('ascii', 'ignore').str.decode('ascii')
        df['engine'] = df['engine'].str.replace('DoubleOverhead Camshaft', 'Double Overhead Camshaft')

        print("Data Preprocessing Successful")
        print(df.head())
        df.to_csv('/opt/airflow/dags/P2M3_ryan_dionenatan_data_clean.csv', index=False)

    @task()
    def post_to_elastic():
        '''
        This function is for post all the data taken from clean CSV into Elasticsearch.
        This function takes no parameters.

        Usage example:

        post_to_elastic()
        '''
        # Read the Raw Data CSV
        df = pd.read_csv('/opt/airflow/dags/P2M3_ryan_dionenatan_data_clean.csv')
        # Initiate Elasticsearch
        es = Elasticsearch("http://elasticsearch:9200")

        # Convert data into Elasticsearch format
        actions = []
        for i, row in df.iterrows():
            actions.append(
                {
                    "_index": "table_m3",
                    "_id": i,
                    "_source": {
                        "car_id": row["car_id"],
                        "date": row["date"],
                        "customer_name": row["customer_name"],
                        "gender": row["gender"],
                        "annual_income": row["annual_income"],
                        "dealer_name": row["dealer_name"],
                        "company": row["company"],
                        "model": row["model"],
                        "engine": row["engine"],
                        "transmission": row["transmission"],
                        "color": row["color"],
                        "price": row["price"],
                        "dealer_no": row["dealer_no"],
                        "body_style": row["body_style"],
                        "phone": row["phone"],
                        "dealer_region": row["dealer_region"]
                    }
                }
            )

        # Post to Elasticsearch
        response = helpers.bulk(es, actions)
        print(response)

    read_from_postgre() >> preprocess_data() >> post_to_elastic()