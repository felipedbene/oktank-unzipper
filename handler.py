import boto3,json


def main(event, context):
    resp = {
        "statusCode": 200,
        "body": json.dumps("hello !")
    }

    return resp
