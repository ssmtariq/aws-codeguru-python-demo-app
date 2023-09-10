import os
import random
import time
import boto3
import gevent

from image_editor import ImageEditor

BW_FOLDER = "bw-images/"
BRIGHTEN_FOLDER = "brighten-images/"


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
    # print("file_path=", file_path)
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
            response = self.sqs_client.receive_message(QueueUrl=self.sqs_queue_url, MaxNumberOfMessages=10)
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
            # print("inside monochrome and upload")
            image_name = source_image.split(".")[-2]
            # print("image_name=", image_name)
            target_file_path = source_image + "-monochrome.png"
            # print("target_file_path=", target_file_path)
            # Construct the full file path inside /tmp directory
            tmp_target_file_path = os.path.join('/tmp', target_file_path)

            try:
                # print("source_image=", source_image)
                # Construct the full file path inside /tmp directory
                tmp_source_image = os.path.join('/tmp', source_image)
                ImageEditor.monochrome(tmp_source_image, tmp_target_file_path)
                # print("Calling upload")
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
            # print("inside brighten and upload")
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
        # print("length of messages is: ", len(messages))
        for image_key in messages:
            try:
                # print(f"Processing image: {image_key}")
                image_name = self._get_name_from_key(image_key)
                print("Image name: " + image_name)
                image_name_without_file_suffix = image_name.split(".")[-2]
                image_file_suffix = image_name.split(".")[-1]
                temp_image_path = image_name_without_file_suffix + "-" + str(random.randrange(100000))
                self._download_image(image_key, temp_image_path + "." + image_file_suffix)
                bw_image_processor.monochrome_and_upload(temp_image_path + "." + image_file_suffix)
                brighten_image_processor.brighten_and_upload(temp_image_path + "." + image_file_suffix)
                delete_file(temp_image_path + "." + image_file_suffix)
                print(f"Finished processing image: {image_key}")
            except Exception as e:
                print(f"Error processing image: {image_key}")
                print(e)

    def concurrent_processing(self, messages, bw_image_processor, brighten_image_processor):
        if len(messages) == 1:
            # Handle the case when there's only one message
            self.process_image(messages, bw_image_processor, brighten_image_processor)
        else:
            greenlets = []
            number_of_greenlets = len(messages)
            # print("Number of messages inside concurrent_processing: ", len(messages))
            # Calculate the number of messages per greenlet
            messages_per_greenlet = len(messages) // number_of_greenlets

            for i in range(number_of_greenlets):
                start_idx = i * messages_per_greenlet
                end_idx = start_idx + messages_per_greenlet
                sub_messages = messages[start_idx:end_idx]
                # print(f"Greenlet {i + 1} processing messages from index {start_idx} to {end_idx - 1}: {sub_messages}")

                greenlet = gevent.spawn(self.process_image, sub_messages, bw_image_processor, brighten_image_processor)
                # Debugging point 2: Print a message when a greenlet is created
                # print(f"Greenlet {i + 1} created.")
                greenlets.append(greenlet)

            # print("Waiting for greenlets to complete...")
            gevent.joinall(greenlets)
            print("All greenlets have completed.")

    def run(self):
        try:
            messages = self._extract_tasks()
            print("Number of messages extracted from SQS: ", len(messages))
            if len(messages) == 0:
                return

            # Call concurrent_processing function
            self.concurrent_processing(messages, self.bw_image_processor, self.brighten_image_processor)
        except Exception as e:
            print("Failed to process message from SQS queue...")
            print(e)
