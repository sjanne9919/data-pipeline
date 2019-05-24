import boto3
import json


def init():
    global ACCESS_KEY, SECRET_KEY, REGION, SERVICE_NAME, QUEUE_NAME, MESSAGE_COUNT, AWS_ACCOUNT, QUEUE_URL

    SERVICE_NAME = "sqs"
    ACCESS_KEY = ""
    SECRET_KEY = ""
    REGION = "us-west-2"
    QUEUE_NAME = "sjanne.fifo"
    MESSAGE_COUNT = "2"
    AWS_ACCOUNT = ""  # 123456789126
    QUEUE_URL = 'https://' + SERVICE_NAME + '.' + REGION + '.amazonaws.com/' + AWS_ACCOUNT + "/" + QUEUE_NAME


def get_client():
    try:
        client = boto3.client(service_name=SERVICE_NAME, aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY, region_name=REGION,
                              endpoint_url='https://' + SERVICE_NAME + '.' + REGION + '.amazonaws.com')
    except Exception as err:
        print("Error while creating " + SERVICE_NAME + " client. " + str(err))
    return client


def get_resource():
    try:
        resource = boto3.resource(service_name=SERVICE_NAME, aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key = SECRET_KEY, region_name=REGION,
                                endpoint_url = 'https://' + SERVICE_NAME + '.' + REGION + '.amazonaws.com')
    except Exception as err:
        print("Error while creating " + SERVICE_NAME + " resource. " + str(err))
    return resource



def purge_sqs_queue():
    try:
        sqsClient = get_client()
        q = sqsClient.purge_queue(QueueUrl=QUEUE_URL)
        print("The queue is purged successfully")
    except Exception as err:
        print(" Error while purging SQS Queue" + str(err))


def receive_messages():
    sqsResource = get_resource()
    initialQueue = sqsResource.get_queue_by_name(QueueName=QUEUE_NAME)
    messages = initialQueue.receive_messages(MaxNumberOfMessages=MESSAGE_COUNT)
    return messages


def send_message(message):
    sqsResource = get_resource()
    initialQueue = sqsResource.get_queue_by_name(QueueName=QUEUE_NAME)
    response = initialQueue.send_message(MessageBody=message, MessageGroupId='documents')
    print("Message Sent to SQS queue", str(response))


# sending Message to SQS queue
message = """{
        "key1": "value1",
        "key2": "value2",
        "key3": "value3" 
    }
"""

send_message(message)

# Receive messages from SQS queue

messages = receive_messages()
for message in messages:
    message_dict = json.loads(message.body)
    print(message_dict)

# Call to purge the Queue

response = purge_sqs_queue()
