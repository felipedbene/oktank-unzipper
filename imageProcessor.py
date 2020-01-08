import boto3
import botocore
import tarfile
import boto.ec2
import boto.utils
import logging
import json
from io import BytesIO

class imageProcessor():

    def __init__(self) :
        self.queue_name = 'files2Unzip'
        self.logger=logging.getLogger()
        self.process_queue = list()
  

    def read_queue(self) :
        sqs = boto3.resource('sqs',region_name='us-east-1')
        # Get the queue
        queue = sqs.get_queue_by_name(QueueName=self.queue_name)

        # Process messages by printing out body and optional author name
        for message in queue.receive_messages():
            message_json = json.loads(message.body)
            message_dict = json.loads(message_json['Message'])
            self.process_queue.append( ( message_dict['Records'][0]['s3']['bucket']['name'], 
            message_dict['Records'][0]['s3']['object']['key'] ) )
            print( "added message")
        print("Finished message queue processing")


    def unzipfiles(self):

        #bucket = event['Records'][0]['s3']['bucket']['name']
        #key = event['Records'][0]['s3']['object']['key']
        s3_client = boto3.client('s3')
        
        for file in self.process_queue :

            bucket = file[0]
            key = file[1]
            
            bucket_dest = 'sm-benfelip-input'
            key_dest_prefix = 'private/' + key.split('/')[0] + '/'
            
            
            print("Doing a body read")
            input_tar_file = s3_client.get_object(Bucket = bucket, Key = key)
            input_tar_content = input_tar_file['Body'].read()
            print"Processing {}".format(input_tar_file))
            with tarfile.open(fileobj = BytesIO(input_tar_content)) as tar:
                for tar_resource in tar:
                    if (tar_resource.isfile()):
                        inner_file_bytes = tar.extractfile(tar_resource).read()
                        print("Uploading {}".format(key_dest_prefix + tar_resource.name))
                        s3_client.upload_fileobj(BytesIO(inner_file_bytes), Bucket = bucket_dest, Key = key_dest_prefix + tar_resource.name)

    def stop_ec2(self):

        conn = boto.ec2.connect_to_region("us-east-1")
        # Get the current instance's id
        my_id = boto.utils.get_instance_metadata()['instance-id']
        print('Stopping EC2 :'+str(my_id))
        conn.stop_instances(instance_ids=[my_id])

if __name__ == "__main__" :
    proceso = imageProcessor()
    while len(proceso.process_queue) == 0 :
        print("Polling for messages")
        proceso.read_queue()
    #proceso.unzipfiles()
    proceso.stop_ec2()
    