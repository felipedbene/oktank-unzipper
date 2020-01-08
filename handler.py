import boto3
import botocore
import tarfile

from io import BytesIO
s3_client = boto3.client('s3')

def lambda_handler(event, context):

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



if __name__ == "__main__":
    lambda_handler()