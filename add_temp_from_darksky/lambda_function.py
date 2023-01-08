import requests
import datetime
import arrow
import json
import boto3
import os

# Gets data from the darksky api
dark_sky_api_key = os.environ["DARKSKY_API_KEY"]
location_coordinates = os.environ["DARKSKY_API_COORDINATES"]

sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName="temperatures")


def lambda_handler(event, context):
    url = "https://api.darksky.net/forecast/{0}/{1}?exclude=minutely,hourly,daily,alerts,flags".format(
        dark_sky_api_key, location_coordinates
    )

    try:
        data = requests.get(url, timeout=3).json()
    except Exception as e:
        print("Exception occured getting data from dark sky")
    else:
        # construct value for queue
        ts = arrow.utcnow()
        data_dict = dict(
            sensor_name="OUTDOOR",
            raw_value=data["currently"]["temperature"],
            status_time=ts.to("America/New_York").strftime("%Y-%m-%d %I:%M:%S %p"),
            status_time_utc=ts.isoformat(),
            current_temperature=True,
        )
        queue.send_message(MessageBody=json.dumps([data_dict]))
        print("Sent message to queue")


# ambda_handler(None, None)
