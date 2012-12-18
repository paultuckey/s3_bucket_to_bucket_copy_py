#!/usr/bin/env python
# encoding: utf-8
"""

Script to sync the contents of one bucket with another.

Useful for copying from a production environment back to a test environment.

"""
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from Queue import LifoQueue
import threading

aws_key = 'xxx'
aws_secret_key = 'yyyy'
srcBucketName = 'AAAAAAA'
dstBucketName = 'BBBBBB'

class Worker(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.conn = S3Connection(aws_key, aws_secret_key)
        self.srcBucket = self.conn.get_bucket(srcBucketName)
        self.dstBucket = self.conn.get_bucket(dstBucketName)
        self.queue = queue

    def run(self):
        while True:
            key_name = self.queue.get()
            k = Key(self.srcBucket, key_name)
            dist_key = Key(self.dstBucket, k.key)
            if not dist_key.exists() or k.etag != dist_key.etag:
                print 'copy: ' + k.key
                self.dstBucket.copy_key(k.key, srcBucketName, k.key, storage_class=k.storage_class)
            else:
                print 'exists and etag matches: ' + k.key

            self.queue.task_done()


def copyBucket(maxKeys = 1000):
    print 'start'

    conn = S3Connection(aws_key, aws_secret_key)
    srcBucket = conn.get_bucket(srcBucketName)

    resultMarker = ''
    q = LifoQueue(maxsize=5000)

    for i in range(10):
        print 'adding worker'
        t = Worker(q)
        t.daemon = True
        t.start()

    while True:
        print 'fetch next 1000, backlog currently at %i' % q.qsize()
        keys = srcBucket.get_all_keys(max_keys = maxKeys, marker = resultMarker)
        for k in keys:
            q.put(k.key)
        if len(keys) < maxKeys:
            print 'Done'
            break
        resultMarker = keys[maxKeys - 1].key

    q.join()
    print 'done'


if __name__ == "__main__":
    copyBucket()


