s3_bucket_to_bucket_copy.py
===========================

Script to copy all files (or files with given prefix) from an AWS S3 Bucket in one region to another bucket in a different region using many
threads in Python.

Add a file, .s3cfg, to your home directory with the following format (or download s3cmd and run "s3cmd --configure"):

> [default]
> access_key = <your access key here>
> secret_key = <your secret key here>

Edit s3_bucket_to_bucket_copy.py with the correct source and destination buckets and your S3 credentials and run.

Then run

> s3_bucket_to_bucket_copy.py <source_bucket_name>[/<prefix>] <dest_bucket_name> 

Developed by us guys at [Showcase Workshop](http://www.showcaseworkshop.com).

Other Contributors:

* Joshua Richardson [Chegg, Inc.](http://chegg.com).
