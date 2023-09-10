import boto3
import random

SAMPLE_IMAGES_FOLDER = "input-images/"



class TaskPublisher:
    def __init__(self, sqs_queue_url, s3_bucket_name):
        self.s3_client = boto3.client('s3')
        self.sqs_client = boto3.client('sqs')
        self.sqs_queue_url = sqs_queue_url
        self.s3_bucket_name = s3_bucket_name

    def _list_image_on_s3(self):
        try:
            print("Listing image in " + self.s3_bucket_name + " under " + SAMPLE_IMAGES_FOLDER)
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket_name, Prefix=SAMPLE_IMAGES_FOLDER)
            print("response=", response)

            objects_in_s3 = list(map(lambda x: x["Key"], response["Contents"]))
            print("Listed image in " + self.s3_bucket_name + " under " + SAMPLE_IMAGES_FOLDER + " successfully.")
            return list(filter(lambda x: x != SAMPLE_IMAGES_FOLDER, objects_in_s3))
        except Exception:
            print("Failed to list images in " + self.s3_bucket_name + " under " + SAMPLE_IMAGES_FOLDER)
            return []


    def _send_sqs_message(self, message):
        try:
            print("sqs_queue_url=", self.sqs_queue_url)
            print("message=", message)
            self.sqs_client.send_message(
                QueueUrl=self.sqs_queue_url,
                MessageBody=message
            )
            print("Sent task to SQS.")
        except Exception:
            print("Failed to send message onto sqs queue")

    def publish_image_transform_task(self, num_of_tasks=10):
        images = self._list_image_on_s3()
        print("length of images = ", len(images))
        print("images = ", images)
        if len(images) == 0:
            print("No images in bucket.")
            return

        print("Start publishing task onto sqs...")
        for i in range(num_of_tasks):
            lucky_number = random.randint(0, len(images) - 1)
            self._send_sqs_message(str(images[lucky_number]))
