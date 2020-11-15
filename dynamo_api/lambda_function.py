import json
from collections import defaultdict

import arrow
import boto3
import pandas as pd

ddb = boto3.client('dynamodb')


def _dt_to_query_keys(dt):
    """
    returns back the partition key, start and end sort keys
    :param dt: arrow dt
    :return: partition dt, start date, end date
    """
    dt = dt.to('UTC')
    dt_partition = dt.strftime('%Y%m')
    dt_start = dt.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    dt_end = dt.replace(hour=23, minute=59, second=59, microsecond=59).isoformat()
    return dt_partition, dt_start, dt_end


def _query_for_dt(location, partition, sd, ed):
    query_results = []
    one_res = ddb.query(TableName='dataTable',
                        KeyConditionExpression="key_name = :k_name AND #ts BETWEEN :st AND :et",
                        ExpressionAttributeValues={
                            ':k_name': {'S': 'temperature+{}+{}'.format(location, partition)},
                            ':st': {'S': sd},
                            ':et': {'S': ed}},
                        ExpressionAttributeNames={'#ts': 'timestamp'},
                        ScanIndexForward=False)
    if one_res['Items']:
        query_results.extend(one_res['Items'])
    return query_results


def _format_query_results(results, resample_freq):
    result_list = []
    if resample_freq:
        df = pd.DataFrame([(a['timestamp']['S'], a['reading_value']['N']) for a in results],
                          columns=['timestamp', 'value']).set_index('timestamp')
        df.loc[:, 'value'] = df['value'].astype(float)
        df.index = pd.to_datetime(df.index)
        df = df.resample(resample_freq).last()

        df.index = df.index.map(lambda x: x.isoformat())
        result_list.extend(df.sort_index(ascending=False).reset_index().to_dict(orient='records'))
    else:
        for r in results:
            result_list.append({'timestamp': r['timestamp']['S'],
                                'value': r['reading_value']['N']
                                })
    return result_list


def get_data_for_range(event, _):
    location = event['pathParameters']['location'].upper()
    ed_str = event['queryStringParameters']['ed_str']
    sd_str = event['queryStringParameters']['sd_str']
    resample_freq = event['queryStringParameters'].get('freq') if event['queryStringParameters'] is not None else None

    partition_ranges = defaultdict(list)  # partition to list of query params
    for dt in pd.date_range(sd_str, ed_str):
        dt_arrow = arrow.get(dt).replace(tzinfo='America/New_York')
        one_part, one_sd, one_ed = _dt_to_query_keys(dt_arrow)
        partition_ranges[one_part].append((one_sd, one_ed))

    query_results = []

    for part_range, params in partition_ranges.items():
        for one_params in params:
            sd, ed = one_params
            query_results.extend(_query_for_dt(location, part_range, sd, ed))

    result_list = _format_query_results(query_results, resample_freq)

    return {
        'statusCode': 200,
        'body': json.dumps({'data': result_list})
    }


def get_data_for_date(event, _):
    location = event['pathParameters']['location'].upper()
    dt_str = event['pathParameters']['date_str']
    resample_freq = event['queryStringParameters'].get('freq') if event['queryStringParameters'] is not None else None

    if dt_str.lower() == 'today':
        dt = arrow.get()
    else:
        dt = arrow.get(dt_str, 'YYYYMMDD').replace(tzinfo='America/New_York')

    dt_partition, sd, ed = _dt_to_query_keys(dt)
    extra_args = {'Limit': 100}
    try:
        extra_args['Limit'] = int(event['queryStringParameters']['limit'])
    except:
        pass

    result_list = _format_query_results(_query_for_dt(location, dt_partition, sd, ed), resample_freq)

    return {
        'statusCode': 200,
        'body': json.dumps({'data': result_list})
    }


# if __name__ == '__main__':
#     import pprint
#
#     evt = {'pathParameters': {'location': 'outdoor', 'date_str': 'today', },
#            'queryStringParameters': {'freq': 'H'}}
#     evt = {'pathParameters': {'location': 'outdoor', 'sd_str': '20201020', 'ed_str': '20201105'},
#            'queryStringParameters': {'freq': 'D'}}
    # pprint.pprint(lambda_handler(evt, None))
    # pprint.pprint(get_data_for_range(evt, None))
