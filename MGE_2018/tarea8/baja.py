#import boto3
#import botocore

#BUCKET_NAME = 'tar7' # replace with your bucket name
#KEY = 'promedio.parquet' # replace with your object key

#s3 = boto3.resource('s3')

#try:
#    s3.Bucket(BUCKET_NAME).download_file(KEY, 'promedio.parquet')
#except botocore.exceptions.ClientError as e:
#    if e.response['Error']['Code'] == "404":
#        print("The object does not exist.")
#    else:
#        raise


import boto3
import os


os.system("aws s3 cp --recursive s3://tar7/promedio.parquet ./")
