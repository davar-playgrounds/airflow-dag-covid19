def create_s3_bucket(s3_resource, bucket_name):
    """
    Create s3 bucket 
    :param s3_resource: s3 resource from boto3
    :param bucket_name: name for the s3 bucket
    :return: success message or already created allert
    """
    try:
        s3_resource.create_bucket(bucket_name)
        print('Bucket {} created !'.format(bucket_name))
    except:
        print('Bucket name already exists !')
