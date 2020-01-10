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


class imageProcessor():

    def __init__(self) :
        self.queue_name = 'files2Unzip'
        self.logger=logging.getLogger()
        self.process_queue = list()
        self.width = 540
        self.height = 390
        self.sorted_video_list = list()
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
        self.purgeQueue()

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
            job_name = key.split("/")[-3]
            self.tmp += "/" + job_name
            self.video_name = job_name

            if not os.path.exists(self.tmp):
                try:
                    os.makedirs(self.tmp)
                except OSError as exc: # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise
            
            print("Doing a body read")
            input_tar_file = s3_client.get_object(Bucket = bucket, Key = key)
            input_tar_content = input_tar_file['Body'].read()
            print("Processing {}".format(input_tar_file))
            with tarfile.open(fileobj = BytesIO(input_tar_content)) as tar:
                for tar_resource in tar:
                    if (tar_resource.isfile()):
                        full_path = os.path.join(self.tmp,tar_resource.name)
                        inner_file_bytes = tar.extractfile(tar_resource).read()
                        print("I", end='')
                        f = open(full_path,"wb")
                        f.write(inner_file_bytes)
                        f.close()
                        
            #Finally render a video with those pics
            self.makeVideo()

    def makeVideo(self):
        image_folder = self.tmp
        sorted_video = os.path.join(self.tmp,self.video_name)+".mp4"
        self.sorted_video_list.append(sorted_video)

        images = [img for img in os.listdir(image_folder) if img.endswith(".jpg")]
        images.sort()
        print("creating video : ", sorted_video)
        # Define the codec and create VideoWriter object
        fourcc = cv2.VideoWriter_fourcc(*'mp4v') # Be sure to use lower case
        video = cv2.VideoWriter(sorted_video , fourcc, 20.0, (self.width, self.height))

        for image in images:
            img = os.path.join(image_folder, image)
            #print(".", end='')
            video.write(cv2.imread(img))

        cv2.destroyAllWindows()
        video.release()

    def sendtoS3(self):
        s3_client = boto3.client('s3')
        print(self.sorted_video_list)
        for item in self.sorted_video_list :
            print("Uploading ",item)
            bucket_dest = 'sm-benfelip-input'
            with open(str(item), "rb") as f:
                key = item.split("/")[-1]
                s3_client.upload_fileobj(f, Bucket = bucket_dest, Key = str("private/" + key ) )
            

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
        print("Polling for messages : ")
        print( str(timer+1), end=' ' )
        proceso.read_queue()
        time.sleep(1)
        timer+=1
    proceso.unzipfiles()
    #proceso.makeVideo()
    proceso.sendtoS3()
    #proceso.stop_ec2()
    