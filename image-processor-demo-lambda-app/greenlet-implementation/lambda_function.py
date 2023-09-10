import json
import os
import logging
import sys

# Path to the libraries stored in your EFS file system
sys.path.append("/mnt/access")

from image_processor import ImageProcessor

# logging.getLogger('botocore').setLevel(logging.DEBUG)

DEMO_APP_SQS_URL = os.environ['DEMO_APP_SQS_URL']
DEMO_APP_BUCKET_NAME = os.environ['DEMO_APP_BUCKET_NAME']


def lambda_handler(event, context):
    sqs_queue_url = DEMO_APP_SQS_URL
    s3_bucket_name = DEMO_APP_BUCKET_NAME
    image_processor = ImageProcessor(sqs_queue_url, s3_bucket_name)
    number_of_extractions = 5  # In each extraction it retrieves 10 messages i.e. for 50 it gets 500 messages

    for i in range(number_of_extractions):
        image_processor.run()  # Run the image processing logic
        print("Number of messages processed: ", ((i + 1) * 10))

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
