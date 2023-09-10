import json
import os

from task_publisher import TaskPublisher

DEMO_APP_SQS_URL = os.environ['DEMO_APP_SQS_URL']
DEMO_APP_BUCKET_NAME = os.environ['DEMO_APP_BUCKET_NAME']


def lambda_handler(event, context):
    sqs_queue_url = DEMO_APP_SQS_URL
    s3_bucket_name = DEMO_APP_BUCKET_NAME
    task_publisher = TaskPublisher(sqs_queue_url, s3_bucket_name)
    # Set number of tasks. If number_of_tasks=10 it will create 100 sqs messages
    number_of_tasks = 10

    for i in range(number_of_tasks):
        task_publisher.publish_image_transform_task()
        print("Total Number of tasks created: ", ((i + 1) * 10))

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
