import json
import boto3
import datetime

ddb = boto3.client('dynamodb')


def lambda_handler(event, context):
    location = event['pathParameters']['location'].upper()
    dt_str = event['pathParameters']['date_str']
    if dt_str.lower() == 'today':
        dt_str = datetime.datetime.utcnow().strftime('%Y%m%d')

    extra_args = {}

    try:
        extra_args['Limit'] = int(event['queryStringParameters']['limit'])
    except:
        pass  # no limit arg

    one_res = ddb.query(TableName='dataTable', KeyConditionExpression="key_name = :k_name",
                        ExpressionAttributeValues={':k_name': {'S': 'temperature+{}+{}'.format(location, dt_str)}},
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
