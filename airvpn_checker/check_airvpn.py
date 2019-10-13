import argparse
import boto3
import json
import uuid
import os
from botocore.vendored import requests
import warnings

sqs = boto3.resource('sqs')
q = sqs.get_queue_by_name(QueueName='low-pri-nagios-alerts.fifo')

def get_airvpn_status(vpn_api_token, expected_session_count):
    url = 'https://airvpn.org/api/'
    exit_code = 0

    resp = requests.get(url, params={'service': 'userinfo',
                                     'format': 'json',
                                     'key': vpn_api_token})
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        message_text = 'Exception when getting status: {}'.format(e)
        exit_code = 2
        return message_text, exit_code

    data = resp.json()
    if 'sessions' not in data:
        message_text = "Could not find any sessions"
        exit_code = 2
    else:
        message_text = 'AirVPN\nSessions currently connected: {}. Expiration date: {}'.format(",".join([d['device_name'] for d in data['sessions']]), data['user']['expiration_date'])
        
        if len(data['sessions']) < expected_session_count:
            exit_code = 2
    # at this point, we have a proper response
    message_text += "\nDays left: {}\nexpiry date={}".format(data['user']['expiration_days'], data['user']['expiration_date'])
    return message_text, exit_code

def lambda_handler(event, context):
    vpn_api_token = os.environ['AIRVPN_TOKEN']
    expected_session_count = int(os.environ['EXPECTED_SESSION_COUNT'])
    status_text, exit_code = get_airvpn_status(vpn_api_token, expected_session_count)
    if exit_code:
        print("sending message")
        q.send_message(MessageBody=json.dumps({'message_text': status_text}), MessageGroupId='nagios-alerts', MessageDeduplicationId=str(uuid.uuid4()))
    print(status_text, exit_code)

if __name__ == '__main__':
    lambda_handler(None)
