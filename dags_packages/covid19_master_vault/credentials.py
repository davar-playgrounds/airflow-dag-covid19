import os
from airflow.models import Variable

import google.auth
import boto3
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1

def get_bqclient():
    """
    Create Big query client utilizaing variable GOOGLE_APPLLICATION_CREDENTIALS
    :return: Big Query Client
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Variable.get('GOOGLE_APPLICATION_CREDENTIALS')
    credentials, my_project_id = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    bqclient = bigquery.Client(
        credentials=credentials,
        project=my_project_id
    )
    return bqclient

def get_bigstorageclient():
    """
    Create Big query Storage client utilizaing variable GOOGLE_APPLLICATION_CREDENTIALS
    :return: Big Query Storage Client
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Variable.get('GOOGLE_APPLICATION_CREDENTIALS')
    credentials, my_project_id = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(
        credentials=credentials
    )
    return bqstorageclient

def get_s3_resource():
    """
    Create s3 resource object to handle S3 bucket interface
    :return: S3 resource handler
    """
    s3_resource = boto3.resource('s3')
    return s3_resource
