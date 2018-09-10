import boto3

client = boto3.client('glue', region_name='us-west-2')

response = client.start_crawler(
    Name='dl-mistar-fm-parquet-partitioned'
)
