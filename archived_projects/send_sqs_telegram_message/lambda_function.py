import boto3
from botocore.vendored import requests
import json
import os
import datetime

sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName="low-pri-nagios-alerts.fifo")


def lambda_handler(event, context):
    telegram_api_key = os.environ["TELEGRAM_API_KEY"]
    telegram_dest_chat_id = os.environ["TELEGRAM_ALERT_CHAT_ID"]

    messages = queue.receive_messages(MaxNumberOfMessages=5, WaitTimeSeconds=2)
    if not messages:
        return

    # list to delete all messages at once
    delete_message_ids = []
    message_to_send = """"""

    for alert_number, message in enumerate(messages):
        try:
            entry = json.loads(message.body)
        except:
            print("Exception while parsing queue data:", body)
        else:
            # construct the text message
            message_to_send += "Message ID: {}\n".format(alert_number)
            message_to_send += entry["message_text"] + "\n"
        delete_message_ids.append(
            {"Id": message.message_id, "ReceiptHandle": message.receipt_handle}
        )

    message_to_send += "--------------------\n"

    queue.delete_messages(Entries=delete_message_ids)

    d = {"chat_id": telegram_dest_chat_id, "text": message_to_send}

    print(
        requests.post(
            "https://api.telegram.org/bot{}/sendMessage".format(telegram_api_key),
            json=d,
        )
    )
