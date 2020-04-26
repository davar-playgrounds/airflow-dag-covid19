from dags_packages.covid19_master_vault.data_handler import DataHandler
from dags_packages.covid19_master_vault.resources import create_s3_bucket
from dags_packages.covid19_master_vault.credentials import get_s3_resource

def data_load_s3():
    #Store data to S3 section
    s3_resource = get_s3_resource()
    bucket_name = 'covid19-datalake'
    create_s3_bucket(s3_resource,bucket_name=bucket_name)
    for source in DataHandler.data_source_list:
        data_handler = DataHandler(source)
        data_handler.upload_s3(bucket_name=bucket_name)


if __name__ == '__main__':
    data_load_s3()
