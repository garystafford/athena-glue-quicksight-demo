import boto3
import os
import logging

# environment variables
data_catalog = os.getenv('DATA_CATALOG')
data_bucket = os.getenv('DATA_BUCKET')

# variables
input_directory = 'sensor_mappings_json'
output_directory = 'sensor_mappings_parquet'

# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# athena client
athena_client = boto3.client('athena')


def handler(event, context):
    return athena_query()


def athena_query():
    query = \
        "CREATE TABLE IF NOT EXISTS " + data_catalog + "." + output_directory + " " \
        "WITH ( " \
        "    format = 'PARQUET', " \
        "    parquet_compression = 'SNAPPY', " \
        "    partitioned_by = ARRAY['state'], " \
        "    external_location = 's3://" + data_bucket + "/" + output_directory + "' " \
        ") AS " \
        "SELECT * " \
        "FROM " + data_catalog + "." + input_directory + ";"

    logger.info(query)

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': data_catalog
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + data_bucket + '/tmp/' + output_directory
        },
        WorkGroup='primary'
    )

    return response
