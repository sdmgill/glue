import boto3
import os

#SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
#S3_FOLDER_INIT = os.environ['S3_FOLDER_INIT']
#S3_FOLDER_INC = os.environ['S3_FOLDER_INC']

SOURCE_BUCKET = 'pa-processed'
S3_FOLDER_INIT = 'mistar/pnct/initial/'
S3_FOLDER_INC = 'mistar/pnct/incremental/'
TARGET_BUCKET = 'pa-processed'

#def lambda_handler(event, context):
s3 = boto3.resource("s3")
s3copy = boto3.client("s3")

searchbucket = s3.Bucket(SOURCE_BUCKET)

# change this string based on the S3 "folder" you want to clean #
sub = S3_FOLDER_INC

object_list = []
word_list = ['LOAD']

for object in searchbucket.objects.all():
    clean_obj = str(object).replace("s3.ObjectSummary(bucket_name='pa-processed', key=", "")
    clean_obj2 = str(clean_obj).replace("'", "")
    clean_obj3 = str(clean_obj2).replace(")", "")
    object_list.append(clean_obj3)


# print(object_list)

# perform the copy to the pa-processed Bucket #
for text in object_list:
    if any(sub in text for sub in word_list):
        copy_source = {'Bucket': SOURCE_BUCKET, 'Key': str(text)}
        copy_target = {'Bucket': TARGET_BUCKET, 'Key': str(text).replace("incremental","initial")}
        s3copy.copy(copy_source,copy_target['Bucket'],copy_target['Key'])
        # s3copy.copy(copy_source,TARGETBUCKET,text)
        #print(copy_source)
        #print(text)

# delete files from Staging after they have been copied to processed #
for cya in object_list:
   if any(sub in cya for sub in word_list):
       s3.Object(SOURCE_BUCKET, cya).delete()


