from __future__ import print_function
import boto3
import logging, inspect
from datetime import  datetime
import pytz


# for local testing set profile
# boto3.setup_default_session(profile_name='nelsone')

current_session=boto3.session.Session()
current_region=current_session.region_name


s3Client = boto3.client('s3')
snsClient = boto3.client('sns')
sns_topic_arn="INSERT_TOPIC_ARN_HERE"

s3bucket="INSERT_S3_BUCKET_HERE"
s3prefix="INSERT_S3_PREFIX_HERE"

today=datetime.now()

'''
This function is used for Logging
to use it, in your function/catch, pass in your Exception and the logging level

Logging levels are:
CRITICAL
ERROR
WARNING
DEBUG
INFO
NOTSET
'''

def log(e, logging_level):
    func_name=inspect.currentframe().f_back.f_code
    logging_level = logging_level.upper()
    print(logging_level+":"+func_name.co_name+":"+str(e))
    
    
''' used to evaluate if any returned structures are empty'''
def is_empty(any_structure):
    if any_structure:
        return False
    else:
        return True


''' this is the main handler for the lambda function '''
def lambda_handler(event, context):
    
    #Make attempts to catch and log exceptions
    try:
        old_files=[]
        utc=pytz.UTC
        today=utc.localize(datetime.now())
                           
        response = s3Client.list_objects_v2(Bucket=s3bucket, Prefix=s3prefix)
        for obj in response['Contents']:
            if obj["LastModified"] < today:
                old_file_info=str(object['LastModified']).split(" ")[0] + "/" + object["Key"]
                old_files.append(old_file_info.split("/")[2])
        
        # We want unique values. We currently have a list of all objects older than today.
        # we need to loop through that and get just the root key (the 'directory' the file is in.)
        filter_key=set(old_files)
        
        for key in filter_key:
            sns_message='WARNING: Found files in sftp-incoming/' + key + ' that are older than today.  There may be an ingestion issue.'
            response=snsClient.publish(TopicArn=sns_topic_arn, Message=sns_message, Subject='WARNING: Possible file ingestion issue.')
            http_response_status=response['ReponseMetadata']['HTTPStatusCode']
            if http_response_status != 200:
                raise ValueError('SNS notification failed to send')
        print("done running")
        
    except Exception as e:
        log(e, 'warning')

if __name__ == '__main__':
    lambda_handler(None, None)
        
        