#!/usr/bin/env python
# encoding: utf-8
"""

Python script to sync the contents of one S3 bucket with another S3 bucket.

This script will work even if the buckets are in different regions.

Useful for copying from a production environment back to a test environment.

python s3_bucket_to_bucket_copy.py src dest > log-s3.log &

tail -f log-s3.log
tail -f log-s3.log | grep Fetch  # show only fetches
grep Fetch log-s3.log

"""
from boto.s3.connection import S3Connection
from Queue import LifoQueue
import threading
import time
import logging
import os
import ConfigParser
import sys

# Log everything, and send it to stderr.
logging.basicConfig(level=logging.WARN)


class Worker(threading.Thread):
    def __init__(self, queue, thread_id, aws_key, aws_secret_key,
                 src_bucket_name, dst_bucket_name, src_path, dst_path):
        threading.Thread.__init__(self)
        self.queue = queue
        self.done_count = 0
        self.thread_id = thread_id
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.src_bucket_name = src_bucket_name
        self.dst_bucket_name = dst_bucket_name
        self.src_path = src_path
        self.dst_path = dst_path
        self.conn = None
        self.src_bucket = None
        self.dest_bucket = None

    def __init_s3(self):
        print '  t%s: conn to s3' % self.thread_id
        self.conn = S3Connection(self.aws_key, self.aws_secret_key)
        self.src_bucket = self.conn.get_bucket(self.src_bucket_name)
        self.dest_bucket = self.conn.get_bucket(self.dst_bucket_name)

    def run(self):
        while True:
            try:
                if self.done_count % 1000 == 0:  # re-init conn to s3 every 1000 copies as we get failures sometimes
                    self.__init_s3()
                key_name = self.queue.get()
                k = self.src_bucket.get_key(key_name)
                dist_key = self.dest_bucket.get_key(k.key)
                if not dist_key or not dist_key.exists() or k.etag != dist_key.etag:
                    print '  t%s: Copy: %s' % (self.thread_id, k.key)
                    acl = self.src_bucket.get_acl(k)
                    self.dest_bucket.copy_key(k.key, self.src_bucket_name, k.key, storage_class=k.storage_class)
                    dist_key = self.dest_bucket.get_key(k.key)
                    dist_key.set_acl(acl)
                else:
                    print '  t%s: Exists and etag matches: %s' % (self.thread_id, k.key)
                self.done_count += 1
            except BaseException:
                logging.exception('  t%s: error during copy' % self.thread_id)
            self.queue.task_done()


def copy_bucket(aws_key, aws_secret_key, src, dst):
    max_keys = 1000

    conn = S3Connection(aws_key, aws_secret_key)
    try:
        (src_bucket_name, src_path) = src.split('/', 1)
    except ValueError:
        src_bucket_name = src
        src_path = None
    try:
        (dst_bucket_name, dst_path) = dst.split('/', 1)
    except ValueError:
        dst_bucket_name = dst
        dst_path = None
    if dst_path is not None:
        raise ValueError("not currently implemented to set dest path; must use default, which will mirror the source")
    src_bucket = conn.get_bucket(src_bucket_name)

    print
    print 'Start copy of %s to %s' % (src, dst)
    print

    result_marker = ''
    q = LifoQueue(maxsize=5000)

    for i in range(20):
        print 'Adding worker thread %s for queue processing' % i
        t = Worker(q, i, aws_key, aws_secret_key,
                   src_bucket_name, dst_bucket_name,
                   src_path, dst_path)
        t.daemon = True
        t.start()

    i = 0

    while True:
        print 'Fetch next %s, backlog currently at %s, have done %s' % \
            (max_keys, q.qsize(), i)
        try:
            keys = src_bucket.get_all_keys(max_keys=max_keys,
                                           marker=result_marker,
                                           prefix=src_path or '')
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

    j = os.path.join
    config_fn = j(os.environ['HOME'], '.s3cfg')
    config = ConfigParser.ConfigParser()
    if not config.read(config_fn):
        raise EnvironmentError("You need to configure s3cmd, 's3cmd --configure'")

    default_aws_key = config.get('default', 'access_key')
    default_aws_secret_key = config.get('default', 'secret_key')

    (src_arg, dest_arg) = sys.argv[1:3]
    copy_bucket(default_aws_key, default_aws_secret_key, src_arg, dest_arg)
