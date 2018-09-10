import boto3
import os

# grab values from Lambda function Environment Variables
SOURCEBUCKET = os.environ['SOURCE_BUCKET']
TARGETBUCKET = os.environ['TARGET_BUCKET']
FOLDER = os.environ['S3_FOLDER']


def lambda_handler(event, context):
    s3 = boto3.resource("s3")
    s3copy = boto3.client("s3")

    searchbucket = s3.Bucket(SOURCEBUCKET)

    # change this string based on the S3 "folder" you want to clean. Determined
    # via the S3_FOLDER Environment Variable #
    sub = FOLDER

    object_list = []

    # place all S3 "folders" in a list and then clean up extraneous values. In
    # the AWS world, S3 does not have any "folders". Instead they use "Buckets"
    # and "Keys". You can think of a "Bucket" as the Parent Folder and the "Key"
    # as the Child Folder
    for object in searchbucket.objects.all():
        clean_obj = str(object).replace("s3.ObjectSummary(bucket_name='pa-dms-staging', key=", "")
        clean_obj2 = str(clean_obj).replace("'", "")
        clean_obj3 = str(clean_obj2).replace(")", "")
        object_list.append(clean_obj3)

    # print(object_list)

    # Perform the copy to the pa-processed Bucket. Since the list generated in
    # the above code will pull all Keys, we filter based on the S3_FOLDER value #
    for text in object_list:
        if sub in text:
            copy_source = {'Bucket': SOURCEBUCKET, 'Key': str(text)}
            copy_target = {'Bucket': TARGETBUCKET, 'Key': str(text)}
            # s3copy.copy(copy_source,copy_target['Bucket'],copy_target['Key'])
            s3copy.copy(copy_source, TARGETBUCKET, text)
            # print(copy_source)
            # print(text)

    # delete files from Staging after they have been copied to processed. Again,
    # we are filtering on the S3_FOLDER value#
    for cya in object_list:
        if sub in cya:
            s3.Object(SOURCEBUCKET, cya).delete()

    return "All clear"
