import os
import threading
import time
import boto3
import random
import sys
sys.path.append("/mnt/access")

import gevent
from gevent import monkey
from skimage import io, exposure
from skimage import img_as_ubyte
from skimage.color import rgb2gray, rgba2rgb

DEMO_APP_SQS_URL = os.environ['DEMO_APP_SQS_URL'] # "https://sqs.REGION.amazonaws.com/ACCOUNT_ID/DemoApplicationQueueLambdaOriginal"
DEMO_APP_BUCKET_NAME = os.environ['DEMO_APP_BUCKET_NAME'] #"python-lambda-imageprocessor-demo-app-test-bucket-original"

BW_FOLDER = "bw-images/"
BRIGHTEN_FOLDER = "brighten-images/"

SAMPLE_IMAGES_FOLDER = "input-images/"
EXAMPLE_IMAGE_LOCAL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../..", "resources", "example-image.png"
)

def _get_environment_variable(key, example_value):
    value = os.getenv(key)
    if value is None:
        raise RuntimeError("Environment variable " + key + " must be set, e.g. " + example_value)
    return value


def print_tmp_files():
    tmp_directory = '/tmp'

    try:
        # List all files in the /tmp directory
        files = os.listdir(tmp_directory)

        if files:
            print("Files in /tmp directory:")
            for file in files:
                print(os.path.join(tmp_directory, file))
        else:
            print("No files found in /tmp directory.")
    except Exception as e:
        print("Error:", str(e))
        print("Failed to list files in /tmp directory.")


def delete_file(file_path):
    print("file_path=", file_path)
    # print_tmp_files()
    # Construct the full file path inside /tmp directory
    tmp_file_path = os.path.join('/tmp', file_path)
    try:
        print("Removing file from " + tmp_file_path)
        os.remove(tmp_file_path)
        print("Successfully removed file from " + tmp_file_path)
    except Exception as e:
        print("Error:", str(e))
        print("Failed to remove file from" + tmp_file_path)


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
        # Construct the full file path inside /tmp directory
        tmp_file_path = os.path.join('/tmp', file_path)
        try:
            print("Downloading " + image_key + " to " + tmp_file_path)
            self.s3_client.download_file(Bucket=self.s3_bucket_name, Key=image_key, Filename=tmp_file_path)
            print("Downloaded " + image_key + " to " + tmp_file_path + " successfully")
        except Exception as e:
            print("Failed to download image " + image_key + " to " + tmp_file_path)
            print("Error:", str(e))
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
            print("inside monochrome and upload")
            image_name = source_image.split(".")[-2]
            print("image_name=", image_name)
            target_file_path = source_image + "-monochrome.png"
            print("target_file_path=", target_file_path)
            # Construct the full file path inside /tmp directory
            tmp_target_file_path = os.path.join('/tmp', target_file_path)

            try:
                print("source_image=", source_image)
                # Construct the full file path inside /tmp directory
                tmp_source_image = os.path.join('/tmp', source_image)
                ImageEditor.monochrome(tmp_source_image, tmp_target_file_path)
                print("Calling upload")
                self._upload_file(tmp_target_file_path, self.s3_bucket_name,
                                  BW_FOLDER + image_name + "-monochrome-" + str(
                                      int(round(time.time() * 1000))) + ".png")
            except Exception as e:
                print("Error in monochrome_and_upload:", str(e))
                raise
            finally:
                delete_file(tmp_target_file_path)

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
            print("inside brighten and upload")
            image_name = source_image.split(".")[-2]
            target_file_path = source_image + "-bright.png"
            # Construct the full file path inside /tmp directory
            tmp_target_file_path = os.path.join('/tmp', target_file_path)

            try:
                # Construct the full file path inside /tmp directory
                tmp_source_image = os.path.join('/tmp', source_image)
                ImageEditor.brighten_image(tmp_source_image, tmp_target_file_path)
                self._upload_file(tmp_target_file_path, self.s3_bucket_name,
                                  BW_FOLDER + image_name + "-bright-" + str(int(round(time.time() * 1000))) + ".png")
            except Exception:
                raise
            finally:
                delete_file(tmp_target_file_path)

    def process_image(self, messages, bw_image_processor, brighten_image_processor):
        for image_key in messages:
            image_name = self._get_name_from_key(image_key)
            print("Image name: " + image_name)
            image_name_without_file_suffix = image_name.split(".")[-2]
            image_file_suffix = image_name.split(".")[-1]
            temp_image_path = image_name_without_file_suffix + "-" + str(random.randrange(100000))
            self._download_image(image_key, temp_image_path + "." + image_file_suffix)
            bw_image_processor.monochrome_and_upload(temp_image_path + "." + image_file_suffix)
            brighten_image_processor.brighten_and_upload(temp_image_path + "." + image_file_suffix)
            delete_file(temp_image_path + "." + image_file_suffix)

    def concurrent_processing(self, messages, bw_image_processor, brighten_image_processor):
        greenlets = []
        number_of_greenlets = 2

        # Calculate the number of messages per greenlet
        messages_per_greenlet = len(messages) // number_of_greenlets

        for i in range(number_of_greenlets):
            start_idx = i * messages_per_greenlet
            end_idx = start_idx + messages_per_greenlet
            sub_messages = messages[start_idx:end_idx]

            greenlets.append(
                gevent.spawn(self.process_image, sub_messages, bw_image_processor, brighten_image_processor))

        gevent.joinall(greenlets)

    def run(self):
        try:
            messages = self._extract_tasks()
            if len(messages) == 0:
                return

            # Call concurrent_processing function
            self.concurrent_processing(messages, self.bw_image_processor, self.brighten_image_processor)
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
            print("response=",response)

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
            print("sqs_queue_url=",self.sqs_queue_url)
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
        print("inside _publish_task")
        self.stop_processing = False
        start_time = time.time()  # Start time of the task_publisher thread
        """
        Publish image transform task every 10 seconds
        """
        while not self.stop_processing:
            print("inside the loop")
            self.task_publisher.publish_image_transform_task()

            time.sleep(10)

            # Check if 14 minutes have passed
            elapsed_time = time.time() - start_time
            if elapsed_time >= 20:
                self.stop_processing = True  # Set the flag to stop processing
                break  # Exit the loop to stop processing immediately

    def _process_message(self):
        print("inside _process_message")
        self.stop_processing = False
        start_time = time.time()  # Start time of the image_processor thread
        """
        Process messages
        """
        while not self.stop_processing:
            print("inside the loop")
            self.image_processor.run()

            # Check if 14 minutes have passed
            elapsed_time = time.time() - start_time
            if elapsed_time >= 20:
                self.stop_processing = True  # Set the flag to stop processing
                break  # Exit the loop to stop processing immediately

    def run(self):
        # Publisher
        self._publish_task()

        # Listener
        self._process_message()


def lambda_handler(event, context):
    SampleDemoApp().run()
