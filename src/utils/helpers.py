import boto3
from botocore.exceptions import NoCredentialsError, ClientError


def get_secret(secret_name, region_name="us-west-2"):
    full_secret_name = f"airflow/variables/{secret_name}"
    client = boto3.client(service_name="secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=full_secret_name)
    return (
        response["SecretString"]
        if "SecretString" in response
        else response["SecretBinary"]
    )


def upload_to_s3(local_file, s3_bucket, s3_file):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(local_file, s3_bucket, s3_file)
        print(f"Upload Successful: {local_file} to {s3_bucket}/{s3_file}")
        return f"s3://{s3_bucket}/{s3_file}"
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as e:
        print(f"Client error: {e}")
        return False
