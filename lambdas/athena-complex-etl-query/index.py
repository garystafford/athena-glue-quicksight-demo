import boto3
import os
import logging
import json
from typing import Dict

# environment variables
data_catalog = os.getenv('DATA_CATALOG')
data_bucket = os.getenv('DATA_BUCKET')

# variables
output_directory = 'etl_tmp_output_parquet'

# uses list comprehension to generate the equivalent of:
# ['s_01', 's_02', ..., 's_09', 's_10']
sensors = [f's_{i:02d}' for i in range(1, 11)]

# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# athena client
athena_client = boto3.client('athena')


def handler(event, context):
    args = {
        "loc_id": event['loc_id'],
        "date_from": event['date_from'],
        "date_to": event['date_to']
    }
    athena_query(args)

    return {
        'statusCode': 200,
        'body': json.dumps("function 'athena-complex-etl-query' complete")
    }


def athena_query(args: Dict[str, str]):
    for sensor in sensors:
        query = \
            "INSERT INTO " + data_catalog + "." + output_directory + " " \
            "WITH " \
            "    t1 AS " \
            "        (SELECT d.loc_id, d.ts, d.data." + sensor + " AS kwh, l.state, l.tz " \
            "        FROM smart_hub_data_catalog.smart_hub_data_parquet d " \
            "        LEFT OUTER JOIN smart_hub_data_catalog.smart_hub_locations_parquet l " \
            "            ON d.loc_id = l.hash " \
            "        WHERE d.loc_id = '" + args['loc_id'] + "' " \
            "            AND d.dt BETWEEN cast('" + args['date_from'] + \
            "' AS date) AND cast('" + args['date_to'] + "' AS date)), " \
            "    t2 AS " \
            "        (SELECT at_timezone(from_unixtime(t1.ts, 'UTC'), t1.tz) AS ts, " \
            "             date_format(at_timezone(from_unixtime(t1.ts, 'UTC'), t1.tz), '%H') AS rate_period, " \
            "             m.description AS device, m.location, t1.loc_id, t1.state, t1.tz, t1.kwh " \
            "        FROM t1 LEFT OUTER JOIN smart_hub_data_catalog.sensor_mappings_parquet m " \
            "            ON t1.loc_id = m.loc_id " \
            "        WHERE t1.loc_id = '" + args['loc_id'] + "' " \
            "            AND m.state = t1.state " \
            "            AND m.description = (SELECT m2.description " \
            "                FROM smart_hub_data_catalog.sensor_mappings_parquet m2 " \
            "                WHERE m2.loc_id = '" + args['loc_id'] + "' AND m2.id = '" + sensor + "')), " \
            "    t3 AS " \
            "        (SELECT substr(r.to, 1, 2) AS rate_period, r.type, r.rate, r.year, r.month, r.state " \
            "        FROM smart_hub_data_catalog.electricity_rates_parquet r " \
            "        WHERE r.year BETWEEN cast(date_format(cast('" + args['date_from'] + \
            "' AS date), '%Y') AS integer) AND cast(date_format(cast('" + args['date_to'] + \
            "' AS date), '%Y') AS integer)) " \
            "SELECT replace(cast(t2.ts AS VARCHAR), concat(' ', t2.tz), '') AS ts, " \
            "    t2.device, t2.location, t3.type, t2.kwh, t3.rate AS cents_per_kwh, " \
            "    round(t2.kwh * t3.rate, 4) AS cost, t2.state, t2.loc_id " \
            "FROM t2 LEFT OUTER JOIN t3 " \
            "    ON t2.rate_period = t3.rate_period " \
            "WHERE t3.state = t2.state " \
            "ORDER BY t2.ts, t2.device;"

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

        logger.info(response)
