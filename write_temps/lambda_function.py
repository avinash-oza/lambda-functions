import json

import arrow
import boto3

ddb = boto3.client('dynamodb')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='temperatures')


def lambda_handler(event, context):
    messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1)
    if not messages:
        print("No messages, nothing to do")
        return

    delete_message_ids = []

    for one_record in messages:
        try:
            entries = json.loads(one_record.body)
        except Exception as e:
            print(e)
            print("Exception when parsing message", one_record)
        else:
            print("Got messages")
            for one_entry in entries:
                dt_obj = arrow.get(one_entry['status_time_utc'])
                dt_str = dt_obj.isoformat()

                key_name = 'temperature+{}+{}'.format(one_entry['sensor_name'].upper(), dt_obj.date().strftime('%Y%m'))
                ddb.put_item(TableName='dataTable', Item={
                    'key_name': {"S": key_name},
                    'timestamp': {"S": dt_str},
                    'reading_value': {"N": str(one_entry['raw_value'])}
                }, ReturnConsumedCapacity='TOTAL')
            delete_message_ids.append({'Id': one_record.message_id, 'ReceiptHandle': one_record.receipt_handle})
    queue.delete_messages(Entries=delete_message_ids)
