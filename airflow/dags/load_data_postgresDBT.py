from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
import logging

from datetime import datetime , timedelta
import pandas as pd
import requests 
import gzip
import io
import os
from tempfile import NamedTemporaryFile

taxi_types = ['fhv']  #,'yellow','fhv'
years= {'green':[2019,2020],
        'yellow':[2020],
        'fhv':[2019]}
months=list(range(1,2))

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Number of retries after first failure
    'retry_delay': timedelta(minutes=10), # Wait 10 mins between retries
    'execution_timeout': timedelta(hours=2)
}

# def extract_data(year, month, taxi_type):
#     URL = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{month:02d}.csv.gz'
#     output_path = f'/opt/airflow/data/{taxi_type}_{year}_{month}.csv'
    
#     try:
#         # 1. Stream download to temporary file
#         with requests.get(URL, stream=True) as response:
#             response.raise_for_status()
            
#             # Create a temporary gzip file
#             with NamedTemporaryFile(delete=False, suffix='.gz') as tmp_gz:
#                 for chunk in response.iter_content(chunk_size=8192):
#                     tmp_gz.write(chunk)
#                 tmp_gz_path = tmp_gz.name

#         # 2. Process in chunks to handle large files
#         schema = get_schema(taxi_type)
#         first_chunk = True
        
#         with gzip.open(tmp_gz_path, 'rb') as gzipped_file:
#             # Read in chunks (50,000 rows at a time)
#             for chunk in pd.read_csv(
#                 gzipped_file,
#                 chunksize=50000,
#                 encoding='latin-1',  # More permissive encoding
#                 low_memory=False
#             ):
#                 # Process each chunk
#                 processed_chunk = process_chunk(chunk, taxi_type, schema)
                
#                 # Write to final CSV incrementally
#                 mode = 'w' if first_chunk else 'a'
#                 header = first_chunk
#                 processed_chunk.to_csv(output_path, mode=mode, header=header, index=False)
#                 first_chunk = False
        
#         # Clean up temporary file
#         os.unlink(tmp_gz_path)
#         logging.info("Extract task has been finsihed")
#         return True
        
#     except Exception as e:
#         # Clean up if error occurs
#         if 'tmp_gz_path' in locals() and os.path.exists(tmp_gz_path):
#             os.unlink(tmp_gz_path)
#         raise Exception(f"Failed to process {taxi_type} {year}-{month}: {str(e)}")

def get_schema(taxi_type):
    """Returns the schema definition for each taxi type"""
    return {
        'green': {
            'required_columns': [
                'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
                'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID',
                'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
                'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',
                'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge'
            ],
            'datetime_cols': ['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
            'optional_columns': ['ehail_fee', 'trip_type', 'congestion_surcharge'],
            'numeric_cols': [
                'passenger_count', 'trip_distance', 'fare_amount', 'extra',
                'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee',
                'improvement_surcharge', 'total_amount'
            ]
        },
        'yellow': {
            'required_columns': [
                'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag',
                'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra',
                'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
                'total_amount', 'congestion_surcharge'
            ],
            'datetime_cols': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            'optional_columns': ['congestion_surcharge'],
            'numeric_cols': [
                'passenger_count', 'trip_distance', 'fare_amount', 'extra',
                'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
                'total_amount'
            ]
        },
        'fhv': {
            'required_columns': [
                'dispatching_base_num', 'pickup_datetime', 'dropOff_datetime',
                'PUlocationID', 'DOlocationID', 'SR_Flag', 'Affiliated_base_number'
            ],
            'datetime_cols': ['pickup_datetime', 'dropOff_datetime'],
            'optional_columns': ['SR_Flag'],
            'numeric_cols': []
        }
    }.get(taxi_type)

def process_chunk(chunk, taxi_type, schema):
    """Processes a single chunk of data"""
    # Select only available columns
    available_cols = [col for col in schema['required_columns'] 
                     if col in chunk.columns or col in schema['optional_columns']]
    chunk = chunk[available_cols]
    
    # Rename columns
    chunk = chunk.rename(columns={
        'green': {
            'lpep_pickup_datetime': 'pickup_datetime',
            'lpep_dropoff_datetime': 'dropoff_datetime',
            'PULocationID': 'pickup_location_id',
            'DOLocationID': 'dropoff_location_id'
        },
        'yellow': {
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'PULocationID': 'pickup_location_id',
            'DOLocationID': 'dropoff_location_id'
        },
        'fhv': {
            'pickup_datetime': 'pickup_datetime',
            'dropOff_datetime': 'dropoff_datetime',
            'PUlocationID': 'pickup_location_id',
            'DOlocationID': 'dropoff_location_id',
            'SR_Flag': 'trip_type',
            'dispatching_base_num': 'vendorid',
            'Affiliated_base_number': 'store_and_fwd_flag'
        }
    }.get(taxi_type, {}))
    
    # Convert datetime columns
    for col in schema['datetime_cols']:
        if col in chunk.columns:
            chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
    
    # Convert numeric columns
    for col in schema.get('numeric_cols', []):
        if col in chunk.columns:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
    
    # Clean data
    chunk.replace(['N', 'nan', 'NaT', '', 'NULL'], pd.NA, inplace=True)
    
    return chunk

def create_table(taxi_type): 
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    pg_hook.run(f"""CREATE TABLE IF NOT EXISTS datatalks.{taxi_type} (
              unique_row_id          text,
              filename               text,
              VendorID               text,
              pickup_datetime   timestamp ,
              dropoff_datetime  timestamp ,
              store_and_fwd_flag     text,
              RatecodeID             text,
              pickup_location_id           text,
              dropoff_location_id           text,
              passenger_count        double precision,
              trip_distance          double precision,
              fare_amount            double precision,
              extra                  double precision,
              mta_tax                double precision,
              tip_amount             double precision,
              tolls_amount           double precision,
              ehail_fee              double precision,
              improvement_surcharge  double precision,
              total_amount           double precision,
              payment_type           text,
              trip_type              text,
              congestion_surcharge   double precision
            );
            """
        )
    
    logging.info("Creation of main table is done!")

def create_table_staging(taxi_type): 
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    pg_hook.run(f"""CREATE TABLE IF NOT EXISTS datatalks.{taxi_type}_staging (
              unique_row_id          text,
              filename               text,
              VendorID               text,
              pickup_datetime   timestamp ,
              dropoff_datetime  timestamp ,
              store_and_fwd_flag     text,
              RatecodeID             text,
              pickup_location_id           text,
              dropoff_location_id           text,
              passenger_count        double precision,
              trip_distance          double precision,
              fare_amount            double precision,
              extra                  double precision,
              mta_tax                double precision,
              tip_amount             double precision,
              tolls_amount           double precision,
              ehail_fee              double precision,
              improvement_surcharge  double precision,
              total_amount           double precision,
              payment_type           double precision,
              trip_type              double precision,
              congestion_surcharge   double precision
          );"""
        )

    logging.info("Creation of staging table is done!")

def delete_staging_data(taxi_type):
    logging.info("Deleting data from staging table is started!")
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    pg_hook.run(f'Truncate table datatalks.{taxi_type}_staging')

    logging.info("deleting data from staging before loading data is done!")

def get_columns_for_taxi_type(taxi_type):
    """Return columns available for each taxi type"""
    common_columns = [
        "pickup_datetime", "dropoff_datetime", "pickup_location_id",
        "dropoff_location_id", "trip_distance", "fare_amount"
    ]
    
    if taxi_type == "green":
        return common_columns + [
            "VendorID", "store_and_fwd_flag", "RatecodeID",
            "passenger_count", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "ehail_fee", "improvement_surcharge",
            "total_amount", "payment_type", "trip_type", "congestion_surcharge"
        ]
    elif taxi_type == "yellow":
        return common_columns + [
            "VendorID", "passenger_count", "RatecodeID",
            "store_and_fwd_flag", "payment_type", "extra",
            "mta_tax", "tip_amount", "tolls_amount",
            "improvement_surcharge", "total_amount"
        ]
    elif taxi_type == "fhv":
        return [
            "dispatching_base_num", "pickup_datetime", "dropoff_datetime",
            "pickup_location_id", "dropoff_location_id", "SR_Flag",
            "Affiliated_base_number"
        ]

def load_data(year, month, taxi_type):
    logging.info("Loading data into staging table is started!")
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    filePath = f'/opt/airflow/data/{taxi_type}_{year}_{month}.csv'
    with open(filePath, 'r') as f:
        header = f.readline().strip().split(',')
    
    # Connect and copy
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        with open(filePath, 'r') as f:
            cursor.copy_expert(f"""
                COPY datatalks.{taxi_type}_staging ({','.join(header)})
                FROM STDIN 
                DELIMITER ',' CSV HEADER NULL AS '';
            """, f)
        conn.commit()
        logging.info("loading data into staging is done!")
    finally:
        cursor.close()
        conn.close()

def update_staging_fnc(taxi_type,month):
        logging.info("Updating staging table data is started!")
        pg_hook=PostgresHook(postgres_conn_id='postgres_conn')
        pg_hook.run(f"""UPDATE datatalks.{taxi_type}_staging
        SET
        unique_row_id = md5(
        COALESCE(CAST(VendorID AS text), '') ||
        COALESCE(CAST(pickup_datetime AS text), '') ||
        COALESCE(CAST(dropoff_datetime AS text), '') ||
        COALESCE(pickup_location_id , '') ||
        COALESCE(dropoff_location_id, '') ||
        COALESCE(CAST(fare_amount AS text), '') ||
        COALESCE(CAST(trip_distance AS text), '')
        ),
        filename = '{taxi_type}_{month}data'
        """)


        logging.info("updating staging table data is done!")

def merging_data_fnc(taxi_type, batch_size=1000000):
    logging.info(f"Merging data into {taxi_type} table from staging table data is started!")
    pg_hook=PostgresHook(postgres_conn_id='postgres_conn')
    query = f"""MERGE INTO datatalks.{taxi_type} AS T
          USING datatalks.{taxi_type}_staging AS S
          ON T.unique_row_id = S.unique_row_id
          WHEN NOT MATCHED THEN
            INSERT (
              unique_row_id, filename, VendorID, pickup_datetime, dropoff_datetime,
              store_and_fwd_flag, RatecodeID, pickup_location_id  , dropoff_location_id, passenger_count,
              trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee,
              improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
            )
            VALUES (
              S.unique_row_id, S.filename, S.VendorID, S.pickup_datetime, S.dropoff_datetime,
              S.store_and_fwd_flag, S.RatecodeID, S.pickup_location_id  , S.dropoff_location_id, S.passenger_count,
              S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee,
              S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge
            );"""
    pg_hook.run(query)
    logging.info(f"Merging data into {taxi_type} table from staging table data is done!")

def process_entire_month(taxi_type, year, month,steps):

    for step in steps:
        try:
            step()
        except Exception as e:
            print(f"Failed {taxi_type} {year}-{month}: {str(e)}")
            raise

def create_all_tables(taxi_type) : 
    step_create_tables= [lambda: create_table(taxi_type),
                        lambda: create_table_staging(taxi_type)]

    try:
        for step in step_create_tables : 
            step()
    except Exception as e:
        print(f"Failed createtion table with this error: {e}")
        raise

with DAG("ny_taxi_loader" , 
        max_active_runs = 2 ,
        max_active_tasks = 2 ,
        concurrency = 3 , 
        default_args = default_args , 
        schedule_interval='@once' , 
        start_date= datetime(2024, 1, 1) ,
        catchup=False) as dag : 

    create_schema = PostgresOperator(
        task_id='create_schema', 
        postgres_conn_id='postgres_conn', 
        sql="CREATE SCHEMA IF NOT EXISTS datatalks"
    )

    prev_taxi_task = create_schema

    for taxi_type in taxi_types :

        with TaskGroup(group_id=f'{taxi_type}_group' , dag=dag) as taxi_group :

            create_tables = PythonOperator(
            task_id = f'create_{taxi_type}_tables',
            python_callable=create_all_tables,
            op_kwargs={'taxi_type': taxi_type},
            )

            prev_month_task = create_tables

            for year in years.get(taxi_type) : 
                for month in months :
                    steps = [ 
                        lambda: delete_staging_data(taxi_type),
                        lambda: load_data(year, month, taxi_type),
                        lambda: update_staging_fnc(taxi_type,month),
                        lambda: merging_data_fnc(taxi_type)
                        ]
                    process_month = PythonOperator(
                        task_id = f"process_{taxi_type}_{year}_{month}",
                        python_callable = process_entire_month,
                        op_kwargs = {
                            'taxi_type':taxi_type,
                            'year':year,
                            'month':month,
                            'steps': steps
                        }
                    )
                    
                    prev_month_task >> process_month
                    prev_month_task = process_month

        prev_taxi_task >> taxi_group
        prev_taxi_task = taxi_group
    




