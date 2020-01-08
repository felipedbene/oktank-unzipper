import boto3
import botocore
import tarfile
import boto.ec2
import boto.utils
import logging
logger=logging.getLogger()

from io import BytesIO
s3_client = boto3.client('s3')

def lambda_handler():

    #bucket = event['Records'][0]['s3']['bucket']['name']
    #key = event['Records'][0]['s3']['object']['key']

    bucket = 'vrp-sagemaker-us-east-1-803186506512'
    key = 'LAMBDA-oktank-logistics2020-01-07-17-16-28/output/output.tar.gz'
    
    bucket_dest = 'sm-benfelip-input'
    key_dest_prefix = 'private/' + key.split('/')[0] + '/'
    
    
    
    input_tar_file = s3_client.get_object(Bucket = bucket, Key = key)
    input_tar_content = input_tar_file['Body'].read()

    with tarfile.open(fileobj = BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if (tar_resource.isfile()):
                inner_file_bytes = tar.extractfile(tar_resource).read()
                s3_client.upload_fileobj(BytesIO(inner_file_bytes), Bucket = bucket_dest, Key = key_dest_prefix + tar_resource.name)

def stop_ec2():

    conn = boto.ec2.connect_to_region("us-east-1")
    # Get the current instance's id
    my_id = boto.utils.get_instance_metadata()['instance-id']
    logger.info(' stopping EC2 :'+str(my_id))
    conn.stop_instances(instance_ids=[my_id])

if __name__ == "__main__":
    lambda_handler()