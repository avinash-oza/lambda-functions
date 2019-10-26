import json
import datetime
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
                try:
                    dt_obj = arrow.get(one_entry['status_time_utc'])
                    print("Got utc timestamp")
                except KeyError:
                    dt_obj = arrow.get(datetime.datetime.strptime(one_entry['status_time'], '%Y-%m-%d %I:%M:%S %p')).replace(tzinfo='America/New_York').to('utc')
                except Exception as e:
                    print("Skipping message due to {}. message was: {}".format(str(e), one_entry))
                else:

                    dt_str = dt_obj.isoformat()

                    key_name = 'temperature+{}+{}'.format(one_entry['sensor_name'].upper(), dt_obj.date().strftime('%Y%m%d'))
                    ddb.put_item(TableName='dataTable', Item={
                        'key_name': {"S": key_name},
                        'timestamp': {"S": dt_str },
                        'reading_value': {"N" : str(one_entry['raw_value']) }
                        }, ReturnConsumedCapacity='TOTAL')
                delete_message_ids.append({'Id': one_record.message_id, 'ReceiptHandle': one_record.receipt_handle})
    queue.delete_messages(Entries=delete_message_ids)


#ambda_handler(test_events, None)
