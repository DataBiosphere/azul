import boto

from boto.s3.key import Key

bucket_name = 'abhancoc-luigi-monitor-touch-files'
touchfile_name = 'test_folder/stuff.txt'
s3 = boto.connect_s3()

# Whatever the bucket's name is, in this case the test bucket
bucket = s3.get_bucket(bucket_name)

k = Key(bucket)
k.key = touchfile_name
contents = k.get_contents_as_string()
print contents