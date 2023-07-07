# For a Detailed explanation, Refer to the blog post "Creating and Running AWS Glue Jobs using Python Boto3 SDK: A Step-by-Step Guide" in my Medium profile https://medium.com/@eswarijayakumar

import boto3

session = boto3.Session(
    aws_access_key_id="<provide aws access key id here>",
    aws_secret_access_key="<provide aws secret key here>",
    region_name="us-east-1"
)

glue_client = session.client('glue')

# Creating an AWS Glue Job
response = glue_client.create_job(
    Name="DataTransformJob",
    Role="arn:aws:iam::897801305272:role/GlueRunRole",
    GlueVersion="4.0",
    Command={
        'Name': 'glueetl',
        'ScriptLocation': 's3://bucketName/fileName.py'
    },
    DefaultArguments={
        "--custom-param": "custom-value"
    })


# Run the Glue Job
run_response = glue_client.start_job_run(JobName="DataTransformJob")
run_id = run_response['JobRunId']

# Fetch the Status of the Glue Job Run
run_status_response = glue_client.get_job_run(JobName="DataTransformJob", RunId=run_id)
status = run_status_response['JobRun']['JobRunState']

print(f'Job Run Status {status}')
