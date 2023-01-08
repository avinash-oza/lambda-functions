import os

import requests


def lambda_handler(event, context):
    telegram_api_key = os.environ["TELEGRAM_API_KEY"]
    telegram_dest_chat_id = os.environ["TELEGRAM_ALERT_CHAT_ID"]

    message = event["Records"][0]["Sns"]["Message"]

    d = {"chat_id": telegram_dest_chat_id, "text": message, "parse_mode": "Markdown"}

    print(
        requests.post(
            "https://api.telegram.org/bot{}/sendMessage".format(telegram_api_key),
            json=d,
        )
    )
