import os
import threading
import time
import boto3
import random

from skimage import io, exposure
from skimage import img_as_ubyte
from skimage.color import rgb2gray, rgba2rgb

DEMO_APP_SQS_URL = os.environ['DEMO_APP_SQS_URL'] # "https://sqs.REGION.amazonaws.com/ACCOUNT_ID/DemoApplicationQueueLambdaOriginal"
DEMO_APP_BUCKET_NAME = os.environ['DEMO_APP_BUCKET_NAME'] #"python-lambda-imageprocessor-demo-app-test-bucket-original"

BW_FOLDER = "bw-images/"
BRIGHTEN_FOLDER = "brighten-images/"

SAMPLE_IMAGES_FOLDER = "input-images/"
EXAMPLE_IMAGE_LOCAL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "resources", "example-image.png"
)

def _get_environment_variable(key, example_value):
    value = os.getenv(key)
    if value is None:
        raise RuntimeError("Environment variable " + key + " must be set, e.g. " + example_value)
    return value


def delete_file(file_path):
    try:
        print("Removing file from " + file_path)
        os.remove(file_path)
        print("Successfully removed file from " + file_path)
    except Exception:
        print("Failed to remove file from" + file_path)


class ImageEditor:
    @staticmethod
    def brighten_image(source_filename, target_filename):
        image = io.imread(source_filename)
        brightened_image = exposure.adjust_gamma(image, 0.1)
        io.imsave(fname=target_filename, arr=img_as_ubyte(brightened_image))

    @staticmethod
    def monochrome(source_filename, target_filename):
        image = io.imread(source_filename)
        image_grey = rgb2gray(rgba2rgb(image))
        io.imsave(fname=target_filename, arr=img_as_ubyte(image_grey))


class ImageProcessor:
    def __init__(self, sqs_queue_url, s3_bucket_name):
        self.sqs_client = boto3.client('sqs')
        self.s3_client = boto3.client('s3')
        self.sqs_queue_url = sqs_queue_url
        self.s3_bucket_name = s3_bucket_name
        self.bw_image_processor = self.BWImageProcessor(self.s3_client, self.sqs_queue_url, self.s3_bucket_name)
        self.brighten_image_processor = self.BrightenImageProcessor(self.s3_client, self.sqs_queue_url,
                                                                    self.s3_bucket_name)

    def _extract_tasks(self):
        try:
            print("Extracting tasks from sqs queue - " + self.sqs_queue_url)
            response = self.sqs_client.receive_message(QueueUrl=self.sqs_queue_url, MaxNumberOfMessages=1)
            if "Messages" not in response:
                print("No messages exists in SQS queue at the moment, retry later.")
                return []
            messages = list(map(lambda x: x["Body"], response["Messages"]))
            print("Extracted tasks from sqs queue successfully")
            return messages
        except Exception:
            print("Failed to extract task from sqs queue - " + str(self.sqs_queue_url))
            raise

    @staticmethod
    def _get_name_from_key(key):
        return key.split("/")[-1]

    def _download_image(self, image_key, file_path):
        try:
            print("Downloading " + image_key + " to " + file_path)
            self.s3_client.download_file(Bucket=self.s3_bucket_name, Key=image_key, Filename=file_path)
            print("Downloaded " + image_key + " to " + file_path + " successfully")
        except Exception:
            print("Failed to download image " + image_key + " to " + file_path)
            raise

    class BWImageProcessor:
        def __init__(self, s3_client, sqs_queue_url, s3_bucket_name):
            self.s3_client = s3_client
            self.sqs_queue_url = sqs_queue_url
            self.s3_bucket_name = s3_bucket_name

        def _upload_file(self, filename, bucket, key):
            try:
                print("Uploading file " + filename + " into " + bucket + " with key: " + key)
                self.s3_client.upload_file(filename, bucket, key)
                print("Uploaded file " + filename + " into " + bucket + " with key: " + key + " successfully")
            except Exception:
                print("Failed to upload file " + filename + " into " + bucket + " with key: " + key)

        def monochrome_and_upload(self, source_image):
            image_name = source_image.split(".")[-2]
            target_file_path = source_image + "-monochrome.png"

            try:
                ImageEditor.monochrome(source_image, target_file_path)
                self._upload_file(target_file_path, self.s3_bucket_name,
                                  BW_FOLDER + image_name + "-monochrome-" + str(
                                      int(round(time.time() * 1000))) + ".png")
            except Exception:
                raise
            finally:
                delete_file(target_file_path)

    class BrightenImageProcessor:
        def __init__(self, s3_client, sqs_queue_url, s3_bucket_name):
            self.s3_client = s3_client
            self.sqs_queue_url = sqs_queue_url
            self.s3_bucket_name = s3_bucket_name

        def _upload_file(self, filename, bucket, key):
            try:
                print("Uploading file " + filename + " into " + bucket + " with key: " + key)
                self.s3_client.upload_file(filename, bucket, key)
                print("Uploaded file " + filename + " into " + bucket + " with key: " + key + " successfully")
            except Exception:
                print("Failed to upload file " + filename + " into " + bucket + " with key: " + key)

        def brighten_and_upload(self, source_image):
            image_name = source_image.split(".")[-2]
            target_file_path = source_image + "-bright.png"

            try:
                ImageEditor.brighten_image(source_image, target_file_path)
                self._upload_file(target_file_path, self.s3_bucket_name,
                                  BW_FOLDER + image_name + "-bright-" + str(int(round(time.time() * 1000))) + ".png")
            except Exception:
                raise
            finally:
                delete_file(target_file_path)

    def run(self):
        try:
            messages = self._extract_tasks()
            if len(messages) == 0:
                return

            for image_key in messages:
                image_name = self._get_name_from_key(image_key)
                print("Image name: " + image_name)
                image_name_without_file_suffix = image_name.split(".")[-2]
                image_file_suffix = image_name.split(".")[-1]
                temp_image_path = image_name_without_file_suffix + "-" + str(random.randrange(100000))
                self._download_image(image_key, temp_image_path + "." + image_file_suffix)
                self.bw_image_processor.monochrome_and_upload(temp_image_path + "." + image_file_suffix)
                self.brighten_image_processor.brighten_and_upload(temp_image_path + "." + image_file_suffix)
                delete_file(temp_image_path + "." + image_file_suffix)
        except Exception as e:
            print("Failed to process message from SQS queue...")
            print(e)


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

            objects_in_s3 = list(map(lambda x: x["Key"], response["Contents"]))
            print("Listed image in " + self.s3_bucket_name + " under " + SAMPLE_IMAGES_FOLDER + " successfully.")
            return list(filter(lambda x: x != SAMPLE_IMAGES_FOLDER, objects_in_s3))
        except Exception:
            print("Failed to list images in " + self.s3_bucket_name + " under " + SAMPLE_IMAGES_FOLDER)
            return []

    def _upload_images_onto_s3(self):
        try:
            print("Uploading example image onto S3")
            self.s3_client.upload_file(Filename=EXAMPLE_IMAGE_LOCAL_PATH, Bucket=self.s3_bucket_name,
                                       Key=SAMPLE_IMAGES_FOLDER + "example-image.png")
            print("Successfully uploaded example image onto S3")
        except Exception:
            print("Failed to upload example image onto S3")
            raise

    def _send_sqs_message(self, message):
        try:
            self.sqs_client.send_message(
                QueueUrl=self.sqs_queue_url,
                MessageBody=message
            )
            print("Sent task to SQS.")
        except Exception:
            print("Failed to send message onto sqs queue")

    def publish_image_transform_task(self, num_of_tasks=10):
        images = self._list_image_on_s3()
        if len(images) == 0:
            print("No images in bucket.")
            return

        print("Start publishing task onto sqs...")
        for i in range(num_of_tasks):
            lucky_number = random.randint(0, len(images)-1)
            self._send_sqs_message(str(images[lucky_number]))


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
            if elapsed_time >= 840:
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
            if elapsed_time >= 840:
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