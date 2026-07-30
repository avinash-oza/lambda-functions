import os
import json
import requests


def lambda_handler(event, context):
    telegram_api_key = os.environ["TELEGRAM_API_KEY"]
    telegram_dest_chat_id = os.environ["TELEGRAM_ALERT_CHAT_ID"]

    message = event["Records"][0]["Sns"]["Message"]
    parse_mode = "Markdown"

    try:
        s3_message = json.loads(message)
    except ValueError:
        pass  # regular message that should pass through
    else:
        # s3 message
        record = s3_message["Records"][0]
        region = record["awsRegion"]
        event_name = record["eventName"]
        event_time = record["eventTime"]
        source_ip = record["requestParameters"]["sourceIPAddress"]
        bucket_name = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        size = record["s3"]["object"]["size"]

        message = f"""
S3 Object:
time={event_time}
region={region}
event_name={event_name}
source_ip={source_ip}
bucket={bucket_name}
key={key}
size={size}
"""
        parse_mode = "html"

    d = {"chat_id": telegram_dest_chat_id, "text": message, "parse_mode": parse_mode}

    print(
        requests.post(
            "https://api.telegram.org/bot{}/sendMessage".format(telegram_api_key),
            json=d,
        ).text
    )


if __name__ == "__main__":
    sample_message = {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "EXAMPLE"},
                "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "example-bucket",
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": "arn:aws:s3:::example-bucket",
                    },
                    "object": {
                        "key": "test%2Fkey",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901",
                    },
                },
            }
        ]
    }

    complete_message = {
        "Records": [
            {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:us-east-1:{{accountId}}:ExampleTopic",
                "Sns": {
                    "Type": "Notification",
                    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:ExampleTopic",
                    "Subject": "example subject",
                    "Message": json.dumps(sample_message),
                    "Timestamp": "1970-01-01T00:00:00.000Z",
                    "SignatureVersion": "1",
                    "Signature": "EXAMPLE",
                    "SigningCertUrl": "EXAMPLE",
                    "UnsubscribeUrl": "EXAMPLE",
                    "MessageAttributes": {
                        "Test": {"Type": "String", "Value": "TestString"},
                        "TestBinary": {"Type": "Binary", "Value": "TestBinary"},
                    },
                },
            }
        ]
    }

    lambda_handler(complete_message, None)
