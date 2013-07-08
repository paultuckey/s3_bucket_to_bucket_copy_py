#!/usr/bin/env python
# encoding: utf-8
"""

Python script to sync the contents of one S3 bucket with another S3 bucket.

This script will work even if the buckets are in different regions.

Useful for copying from a production environment back to a test environment.

python s3_bucket_to_bucket_copy.py > log-s3.log &

tail -f log-s3.log
tail -f log-s3.log | grep Fetch  # show only fetches
grep Fetch log-s3.log

"""
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from Queue import LifoQueue
import threading
import time
import logging

default_aws_key = 'xxx'
default_aws_secret_key = 'yyyy'

default_src_bucket_name = 'AAAAAAA'
default_dst_bucket_name = 'BBBBBB'

# Log everything, and send it to stderr.
logging.basicConfig(level=logging.WARN)

class Worker(threading.Thread):
    def __init__(self, queue, thread_id, aws_key, aws_secret_key, src_bucket_name, dst_bucket_name):
        threading.Thread.__init__(self)
        self.queue = queue
        self.done_count = 0
        self.thread_id = thread_id
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.src_bucket_name = src_bucket_name
        self.dst_bucket_name = dst_bucket_name

    def __init_s3(self):
        print '  t%s: conn to s3' % self.thread_id
        self.conn = S3Connection(self.aws_key, self.aws_secret_key)
        self.srcBucket = self.conn.get_bucket(self.src_bucket_name)
        self.dstBucket = self.conn.get_bucket(self.dst_bucket_name)

    def run(self):
        while True:
            try:
                if self.done_count % 1000 == 0:  # re-init conn to s3 every 1000 copies as we get failures sometimes
                    self.__init_s3()
                key_name = self.queue.get()
                k = Key(self.srcBucket, key_name)
                dist_key = Key(self.dstBucket, k.key)
                if not dist_key.exists() or k.etag != dist_key.etag:
                    print '  t%s: Copy: %s' % (self.thread_id, k.key)
                    acl = self.srcBucket.get_acl(k)
                    self.dstBucket.copy_key(k.key, self.src_bucket_name, k.key, storage_class=k.storage_class)
                    dist_key.set_acl(acl)
                else:
                    print '  t%s: Exists and etag matches: %s' % (self.thread_id, k.key)
                self.done_count += 1
            except BaseException:
                logging.exception('  t%s: error during copy' % self.thread_id)
            self.queue.task_done()


def copy_bucket(aws_key, aws_secret_key, src_bucket_name, dst_bucket_name):
    print
    print 'Start copy of %s to %s' % (src_bucket_name, dst_bucket_name)
    print
    max_keys = 1000

    conn = S3Connection(aws_key, aws_secret_key)
    srcBucket = conn.get_bucket(src_bucket_name)

    result_marker = ''
    q = LifoQueue(maxsize=5000)

    for i in range(20):
        print 'Adding worker thread %s for queue processing' % i
        t = Worker(q, i, aws_key, aws_secret_key, src_bucket_name, dst_bucket_name)
        t.daemon = True
        t.start()

    i = 0

    while True:
        print 'Fetch next %s, backlog currently at %s, have done %s' % (max_keys, q.qsize(), i)
        try:
            keys = srcBucket.get_all_keys(max_keys=max_keys, marker=result_marker)
            if len(keys) == 0:
                break
            for k in keys:
                i += 1
                q.put(k.key)
            print 'Added %s keys to queue' % len(keys)
            if len(keys) < max_keys:
                print 'All items now in queue'
                break
            result_marker = keys[max_keys - 1].key
            while q.qsize() > (q.maxsize - max_keys):
                time.sleep(1)  # sleep if our queue is getting too big for the next set of keys
        except BaseException:
            logging.exception('error during fetch, quitting')
            break

    print 'Waiting for queue to be completed'
    q.join()
    print
    print 'Done'
    print


if __name__ == "__main__":
    copy_bucket(default_aws_key, default_aws_secret_key, default_src_bucket_name, default_dst_bucket_name)
