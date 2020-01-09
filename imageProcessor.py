import boto3
import botocore
import tarfile
import boto.ec2
import boto.utils
import logging
import json
from io import BytesIO
import time
import cv2
import os
# from cStringIO import StringIO
import numpy as np

class imageProcessor():

    def __init__(self) :
        self.queue_name = 'files2Unzip'
        self.logger=logging.getLogger()
        self.process_queue = list()
        self.video_name = 'felideo.mp4'
        self.width = 540
        self.height = 390
        self.tmp = '/tmp'
  

    def read_queue(self) :
        sqs = boto3.resource('sqs',region_name='us-east-1')
        # Get the queue
        self.queue = sqs.get_queue_by_name(QueueName=self.queue_name)

        # Process messages by printing out body and optional author name
        for message in self.queue.receive_messages():
            message_json = json.loads(message.body)
            message_dict = json.loads(message_json['Message'])
            self.process_queue.append( ( message_dict['Records'][0]['s3']['bucket']['name'], 
            message_dict['Records'][0]['s3']['object']['key'] ) )
            print( "added message")
            
        print("Finished polling loop")

    def purgeQueue(self):
        self.queue.purge()
        print("Queue purged")

    def unzipfiles(self):

        #bucket = event['Records'][0]['s3']['bucket']['name']
        #key = event['Records'][0]['s3']['object']['key']
        s3_client = boto3.client('s3')

        for file in self.process_queue :

            bucket = file[0]
            key = file[1]
            tmp_folder_name = key.split("/")[-1]
            print(tmp_folder_name)
            
            print("Doing a body read")
            input_tar_file = s3_client.get_object(Bucket = bucket, Key = key)
            input_tar_content = input_tar_file['Body'].read()
            print("Processing {}".format(input_tar_file))
            with tarfile.open(fileobj = BytesIO(input_tar_content)) as tar:
                for tar_resource in tar:
                    if (tar_resource.isfile()):
                        inner_file_bytes = tar.extractfile(tar_resource).read()
                        print("Inflating file {} to {}".format(tar_resource.name,self.tmp))
                        f = open(os.path.join(self.tmp,tmp_folder_name,tar_resource.name),"wb")
                        f.write(inner_file_bytes)
                        f.close()
    def makeVideo():
        image_folder = '.'
        video_name = self.

        # Define the codec and create VideoWriter object
        fourcc = cv2.VideoWriter_fourcc(*'mp4v') # Be sure to use lower case
        video = cv2.VideoWriter(self.video_name, fourcc, 20.0, (self.width, self.height))

    def sendtoS3():

        bucket_dest = 'sm-benfelip-input'
        key_dest_prefix = 'private/' + key.split('/')[0] + '/'
        s3_client.upload_fileobj(BytesIO(inner_file_bytes), Bucket = bucket_dest, Key = key_dest_prefix + tar_resource.name)
        self.purgeQueue()

    def stop_ec2(self):

        conn = boto.ec2.connect_to_region("us-east-1")
        # Get the current instance's id
        my_id = boto.utils.get_instance_metadata()['instance-id']
        print('Stopping EC2 :'+str(my_id))
        conn.stop_instances(instance_ids=[my_id])

if __name__ == "__main__" :
    timer = 0
    proceso = imageProcessor()
    while len(proceso.process_queue) == 0 and timer < 30 :
        print("Polling for messages : " + str(timer+1) )
        proceso.read_queue()
        time.sleep(1)
        timer+=1
    #proceso.unzipfiles()
    proceso.makeVideo()
    #proceso.stop_ec2()
    