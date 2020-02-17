import json

import arrow
import boto3

ddb = boto3.client('dynamodb')


def dt_to_query_keys(dt):
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


def lambda_handler(event, context):
    location = event['pathParameters']['location'].upper()
    dt_str = event['pathParameters']['date_str']
    if dt_str.lower() == 'today':
        dt = arrow.get()
    else:
        dt = arrow.get(dt_str, 'YYYYMMDD').replace(tzinfo='America/New_York')

    dt_partition, sd, ed = dt_to_query_keys(dt)
    extra_args = {'Limit': 100}
    try:
        extra_args['Limit'] = int(event['queryStringParameters']['limit'])
    except:
        pass

    one_res = ddb.query(TableName='dataTable', KeyConditionExpression="key_name = :k_name AND #ts BETWEEN :st AND :et",
                        ExpressionAttributeValues={':k_name': {'S': 'temperature+{}+{}'.format(location, dt_partition)},
                                                   ':st': {'S': sd},
                                                   ':et': {'S': ed}},
                        ExpressionAttributeNames={'#ts': 'timestamp'},
                        ScanIndexForward=False, **extra_args)
    result_list = []
    for r in one_res['Items']:
        result_list.append({'timestamp': r['timestamp']['S'],
                            'value': r['reading_value']['N']
                            })

    return {
        'statusCode': 200,
        'body': json.dumps({'data': result_list})
    }

# if __name__ == '__main__':
#     import pprint
#     evt = {'pathParameters': {'location': 'outdoor', 'date_str': '20200212'}}
#     # evt = {'pathParameters': {'location': 'outdoor', 'date_str': 'today'}}
#     pprint.pprint(lambda_handler(evt, None))
