import requests
import pandas as pd
import os
import json
import boto3


BASE_URL = "https://api.transport.nsw.gov.au/v1/traffic_volume"
API_KEY = os.environ.get('TRANSPORTNSW_APIKEY', 'blank')
BUCKET = os.environ.get('BUCKET_NAME', 'blank')
KEY_PREFIX = 'transportnsw/trafficvolume/landing/'

def get_traffic_data(start_date, end_date):
    headers = {
        "accept": "application/json",
        "Authorization": f"apikey {API_KEY}"
    }
    params = {
        "format": "json",
        "q": f'select * from road_traffic_counts_hourly_permanent where date("date") between \'{start_date}\' and \'{end_date}\''
    }

    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()

    except Exception as e:
        raise e

    return response


def upload_to_s3(file):
    key = KEY_PREFIX + file
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file, BUCKET, key)
        print(f"File {file} uploaded to {BUCKET}/{key}")
    
    except Exception as e:
        print(f"Failed to upload {file} to {BUCKET}/{key}: {e}")


def extract_and_load_to_s3(start_date, end_date):
    filename = f'traffic_volume_{start_date[:-3]}'
    filename_json = filename + '.json'
    filename_pq = filename + '.parquet'

    response = get_traffic_data(start_date, end_date)

    # write data to file
    json_data = response.json()['rows']

    with open(filename_json, 'w') as f:
        json.dumps(json_data, f, indent=2)

    # read as pd and clean up data
    df = pd.read_json(filename_json,convert_dates=['date', 'updated_on'])

    hours_fillna={'hour_00': 0, 'hour_01': 0, 'hour_02': 0, 'hour_03': 0, 'hour_04': 0, 'hour_05': 0, 'hour_06': 0, 'hour_07': 0, 'hour_08': 0, 'hour_09': 0, 'hour_10': 0, 'hour_11': 0, 'hour_12': 0, 'hour_13': 0, 'hour_14': 0, 'hour_15': 0, 'hour_16': 0, 'hour_17': 0, 'hour_18': 0, 'hour_19': 0, 'hour_20': 0, 'hour_21': 0, 'hour_22': 0, 'hour_23': 0}
    df = df.fillna(value=hours_fillna)

    df.astype({
        'cartodb_id': 'int64',
        'the_geom': 'float64',
        'the_geom_webmercator': 'float64',
        'record_id': 'float64',
        'station_key': 'int64',
        'traffic_direction_seq': 'uint8',
        'cardinal_direction_seq': 'uint8',
        'classification_seq': 'uint8',
        'date': 'datetime64[ns, UTC]',
        'year': 'uint16',
        'month': 'uint8',
        'day_of_week': 'uint8',
        'public_holiday': 'bool',
        'school_holiday': 'bool',
        'daily_total': 'int64',
        'hour_00': 'uint8',
        'hour_01': 'uint8',
        'hour_02': 'uint8',
        'hour_03': 'uint8',
        'hour_04': 'uint8',
        'hour_05': 'uint8',
        'hour_06': 'uint8',
        'hour_07': 'uint8',
        'hour_08': 'uint8',
        'hour_09': 'uint8',
        'hour_10': 'uint8',
        'hour_11': 'uint8',
        'hour_12': 'uint8',
        'hour_13': 'uint8',
        'hour_14': 'uint8',
        'hour_15': 'uint8',
        'hour_16': 'uint8',
        'hour_17': 'uint8',
        'hour_18': 'uint8',
        'hour_19': 'uint8',
        'hour_20': 'uint8',
        'hour_21': 'uint8',
        'hour_22': 'uint8',
        'hour_23': 'uint8',
        'md5': 'object',
        'updated_on': 'datetime64[ns, UTC]'
    })

    df.to_parquet(filename_pq, engine='pyarrow', index=False)

    # upload to s3
    key = KEY_PREFIX + filename_pq
    upload_to_s3(filename_pq)

