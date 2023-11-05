import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from zipfile import ZipFile
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import geopandas
import boto3
from io import StringIO

aws_access_key_id='your_aws_access_key_id'
aws_secret_access_key='your_aws_secret_access_key'
s3 = boto3.client('s3', 
                  aws_access_key_id=aws_access_key_id, 
                  aws_secret_access_key=aws_secret_access_key)
s3_bucket = 'airflow-data-source'

# from google.cloud import storage
dag = DAG(
    dag_id="nyc_trans_demo",
    start_date=airflow.utils.dates.days_ago(7),
    schedule_interval=None,
    default_args={
        'retries': 1,  # Three retries
        'retry_delay': timedelta(seconds=2),  # 5 seconds between retries
    },
)


download_citibike = BashOperator(
    task_id="download_citibike",
    bash_command="curl -o /tmp/202302-citibike.zip -L 'https://s3.amazonaws.com/tripdata/202302-citibike-tripdata.csv.zip'",
    dag=dag,
)

download_yellowtaxi = BashOperator(
    task_id="download_yellowtaxi",
    bash_command="curl -o /tmp/yellow_tripdata_2023-02.parquet -L 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet'",
    dag=dag,
)

download_zones = BashOperator(
    task_id="download_zones",
    bash_command="curl -o /tmp/taxi_zones.zip -L 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip'",
    dag=dag,
)

def _load_weather_data():
    s3_object_key = "nyc_weather_02.csv"
    response = s3.get_object(Bucket=s3_bucket, Key=s3_object_key)
    data = response['Body'].read()
    csv_data = data.decode('utf-8')
    weather_info = pd.read_csv(StringIO(csv_data))

    # Create a SnowflakeHook using the Airflow connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # Define the SQL insert statement
    table_name = 'weather'
    insert_sql = f"INSERT INTO {table_name} (DATE, AWND, PRCP, TAVG) VALUES (%s, %s, %s, %s)"

    # Create a list of tuples containing the values to be inserted
    values = []
    for _, row in weather_info.iterrows():
        values.append((row['DATE'], row['AWND'], row['PRCP'], row['TAVG']))

    cursor.executemany(insert_sql, values)
    cursor.execute("COMMIT")
    cursor.close()

load_weathers_into_snowflake = PythonOperator(
    task_id="load_weathers_into_snowflake", python_callable=_load_weather_data, dag=dag
)

def _load_zones_into_snowflake():
    taxi_zones = geopandas.read_file("/tmp/taxi_zones.zip").to_crs("EPSG:4326")

    # Create a SnowflakeHook using the Airflow connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # Define the SQL insert statement
    table_name = 'zone'
    insert_sql = f"INSERT INTO {table_name} (LocationID, zone) VALUES (%s, %s)"

    # Create a list of tuples containing the values to be inserted
    values = []
    for _, row in taxi_zones.iterrows():
        values.append((row['LocationID'], row['zone']))

    cursor.executemany(insert_sql, values)
    cursor.execute("COMMIT")
    cursor.close()

load_zones_into_snowflake = PythonOperator(
    task_id="load_zones_into_snowflake", python_callable=_load_zones_into_snowflake, dag=dag
)

# Define the URLs and file paths
download_file = '/tmp/202302-citibike.zip'
unzip_dir = '/tmp'

# Function to unzip the file
def _unzip_citibike_data():
    with ZipFile(download_file, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)


unzip_citibike = PythonOperator(
    task_id="unzip_citibike",
    python_callable=_unzip_citibike_data,
    dag=dag,
)

def _load_citibike_into_snowflake():
    df = pd.read_csv('/tmp/202302-citibike-tripdata.csv',nrows=2000)
    df['started_at'] = pd.to_datetime(df['started_at'])
    df['ended_at'] = pd.to_datetime(df['ended_at'])
    df['duration'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    df['duration'] = df['duration'].round(1)
    df['started_at'] = df['started_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['ended_at'] = df['ended_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    taxi_zones = geopandas.read_file("/tmp/taxi_zones.zip").to_crs("EPSG:4326")
    # Create GeoDataFrames for both start and end stations
    start_gdf = geopandas.GeoDataFrame(
        df,
        crs="EPSG:4326",
        geometry=geopandas.points_from_xy(df["start_lng"], df["start_lat"])
    )

    end_gdf = geopandas.GeoDataFrame(
        df,
        crs="EPSG:4326",
        geometry=geopandas.points_from_xy(df["end_lng"], df["end_lat"])
    )

    # Perform a spatial join for start stations to map them to taxi regions
    start_mapped = geopandas.sjoin(start_gdf, taxi_zones, how="inner")
    df['start_region'] = start_mapped['LocationID']

    # Perform a spatial join for end stations to map them to taxi regions
    end_mapped = geopandas.sjoin(end_gdf, taxi_zones, how="inner")
    df['end_region'] = end_mapped['LocationID']

    # Create a SnowflakeHook using the Airflow connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # Define the SQL insert statement
    table_name = 'citibike'
    insert_sql = f"INSERT INTO {table_name} (ride_id, rideable_type, started_at, ended_at, duration, \
                                            start_region, start_station_name, start_station_id, \
                                            end_region, end_station_name, end_station_id, \
                                            start_lat, start_lng, \
                                            end_lat, end_lng, member_casual) \
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # Create a list of tuples containing the values to be inserted
    values = []
    for _, row in df.iterrows():
        values.append((row['ride_id'], row['rideable_type'], str(row['started_at']), \
                       row['ended_at'], row['duration'], row['start_region'], \
                       row['start_station_name'], row['start_station_id'], row['end_region'], \
                       row['end_station_name'], row['end_station_id'], \
                       row['start_lat'], row['start_lng'], \
                       row['end_lat'], row['end_lng'], row['member_casual']))

    cursor.executemany(insert_sql, values)
    cursor.execute("COMMIT")
    cursor.close()

load_citibike_into_snowflake = PythonOperator(
    task_id="load_citibike_into_snowflake", python_callable=_load_citibike_into_snowflake, dag=dag
)

def _load_yellowtaxi_into_snowflake():
    df_yt = pd.read_parquet('/tmp/yellow_tripdata_2023-02.parquet').head(2000)
    df_yt['duration'] = (df_yt['tpep_dropoff_datetime'] - df_yt['tpep_pickup_datetime']).dt.total_seconds() / 60
    df_yt['duration'] = df_yt['duration'].round(1)
    df_yt['tpep_pickup_datetime'] = df_yt['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_yt['tpep_dropoff_datetime'] = df_yt['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Create a SnowflakeHook using the Airflow connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # Define the SQL insert statement
    table_name = 'yellow_taxi'
    columns = ', '.join(df_yt.columns)
    values_placeholder = ', '.join(['%s' for _ in df_yt.columns])
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"

    # Create a list of tuples containing the values to be inserted
    values = [tuple(row) for row in df_yt.itertuples(index=False)]

    cursor.executemany(insert_sql, values)
    cursor.execute("COMMIT")
    cursor.close()

load_yellowtaxi_into_snowflake = PythonOperator(
    task_id="load_yellowtaxi_into_snowflake", python_callable=_load_yellowtaxi_into_snowflake, dag=dag
)

# Set the task dependencies
download_citibike >> download_zones >> unzip_citibike >> load_citibike_into_snowflake
download_zones >> load_zones_into_snowflake
download_yellowtaxi >> load_yellowtaxi_into_snowflake
load_weathers_into_snowflake