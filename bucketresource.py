import boto3
import time
import os


class BucketResource:
    def __init__(self, ak, sk, host):
        self.access_key = ak
        self.secret_key = sk
        self.s3_host = host

        # NO BUCKET NAME in endpoint url even in virtual hosted style

        # s3config= botocore.client.Config(s3={"addressing_style": "virtual"})
        # config=Config(s3={'addressing_style': 'virtual'})

        # Have to use virtual hosted style
        # by setting the config
        self.s3client = boto3.client('s3',
                                     aws_secret_access_key=self.secret_key,
                                     aws_access_key_id=self.access_key,
                                     endpoint_url=self.s3_host,
                                     )

    def list_buckets(self):
        buckets = self.s3client.list_buckets()['Buckets']
        return buckets

    def get_bucket(self, bucket_name):
        try:
            bucket = self.s3client.head_bucket(Bucket=bucket_name)
            return bucket
        except Exception as e:
            # print bucket_name + ' ' + e.message
            return None

    def create_bucket(self, bucket_name):
        try:
            response = self.s3client.create_bucket(Bucket=bucket_name)
            # print response
            return True
        except Exception as e:
            # print bucket_name + ' ' + e.message
            return False


def main():
    access_key = os.environ['S3AK'] if 'S3AK' in os.environ else 'RUEIDZ5X943KB73HPST3'
    secret_key = os.environ['S3SK'] if 'S3SK' in os.environ else 'HXxjNM7oEwgu3LvNZqCDHSLwYlyZLgugCK8NHBRC'
    s3_host = os.environ['S3HOST'] if 'S3HOST' in os.environ else 'http://10.91.243.202'

    s3resource = BucketResource(access_key, secret_key, s3_host)
    bucket_name = 'archive-' + time.strftime("%Y%W")

    if not s3resource.get_bucket(bucket_name):
        s3resource.create_bucket(bucket_name)


if __name__ == "__main__":
    main()
