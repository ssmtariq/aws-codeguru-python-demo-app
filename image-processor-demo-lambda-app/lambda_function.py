import os
import threading
import time
import logging
import sys

# Path to the libraries stored in your EFS file system
sys.path.append("/mnt/access/python")

from image_processor import ImageProcessor
from task_publisher import TaskPublisher

logging.getLogger('botocore').setLevel(logging.DEBUG)

# "https://sqs.REGION.amazonaws.com/ACCOUNT_ID/DemoApplicationQueueLambdaOriginal"
DEMO_APP_SQS_URL = os.environ['DEMO_APP_SQS_URL']
# "python-lambda-imageprocessor-demo-app-test-bucket-original"
DEMO_APP_BUCKET_NAME = os.environ['DEMO_APP_BUCKET_NAME']


def _get_environment_variable(key, example_value):
    value = os.getenv(key)
    if value is None:
        raise RuntimeError("Environment variable " + key + " must be set, e.g. " + example_value)
    return value


class SampleDemoApp:
    def __init__(self):
        self.sqs_queue_url = DEMO_APP_SQS_URL
        self.s3_bucket_name = DEMO_APP_BUCKET_NAME
        self.task_publisher = TaskPublisher(self.sqs_queue_url, self.s3_bucket_name)
        self.image_processor = ImageProcessor(self.sqs_queue_url, self.s3_bucket_name)

    def _publish_task(self):
        self.stop_processing = False
        start_time = time.time()  # Start time of the task_publisher thread
        """
        Setup a thread to publish 10 image transform task every 10 seconds
        """
        while not self.stop_processing:
            task_thread = threading.Thread(target=self.task_publisher.publish_image_transform_task,
                                           name="task-publisher")
            task_thread.start()
            task_thread.join()
            time.sleep(10)

            # Check if 14 minutes have passed
            elapsed_time = time.time() - start_time
            if elapsed_time >= 20:
                self.stop_processing = True  # Set the flag to stop processing
                break  # Exit the loop to stop processing immediately

    def _process_message(self):
        self.stop_processing = False
        start_time = time.time()  # Start time of the image_processor thread
        """
        Setup a thread to process message
        """
        while not self.stop_processing:
            task_thread = threading.Thread(target=self.image_processor.run, name="task-publisher")
            task_thread.start()
            task_thread.join()

            # Check if 14 minutes have passed
            elapsed_time = time.time() - start_time
            if elapsed_time >= 20:
                self.stop_processing = True  # Set the flag to stop processing
                break  # Exit the loop to stop processing immediately

    def run(self):
        # Publisher
        task_publisher_thread = threading.Thread(target=self._publish_task, name="task_publisher_scheduler")
        task_publisher_thread.start()

        # Listener
        task_processor_thread = threading.Thread(target=self._process_message(), name="task_processor_thread")
        task_processor_thread.start()


def lambda_handler(event, context):
    SampleDemoApp().run()
