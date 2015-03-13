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
import argparse

# Log everything, and send it to stderr.
logging.basicConfig(level=logging.WARN)


class Worker(threading.Thread):
    def __init__(self, queue, thread_id, aws_key, aws_secret_key,
                 src_bucket_name, dst_bucket_name, src_path, dst_path, args):
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
        self.args = args

    def __init_s3(self):
        if self.args.verbose:
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
                    if self.args.quiet == False:
                        print '  t%s: Copy: %s' % (self.thread_id, k.key)

                    if self.args.rr:
                        s_class = 'REDUCED_REDUNDANCY';
                    else:
                        s_class = k.storage_class

                    key_suffix = k.key[len(self.src_path):]
                    new_key = self.dst_path + key_suffix if self.dst_path[-1] == "/" else self.dst_path + "/" + key_suffix
                    self.dest_bucket.copy_key(new_key, self.src_bucket_name, k.key, storage_class=s_class, encrypt_key=self.args.encrypt)

                    if self.args.acl == False:
                        acl = self.src_bucket.get_acl(k)
                        dist_key = self.dest_bucket.get_key(new_key)
                        dist_key.set_acl(acl)
                else:
                    if self.args.verbose:
                        print '  t%s: Exists and etag matches: %s' % (self.thread_id, new_key)
                self.done_count += 1
            except BaseException:
                logging.exception('  t%s: error during copy' % self.thread_id)
            self.queue.task_done()


def copy_bucket(aws_key, aws_secret_key, args):
    max_keys = 1000

    src = args.src_bucket
    dst = args.dest_bucket

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
    src_bucket = conn.get_bucket(src_bucket_name)

    if args.verbose:
        print
        print 'Start copy of %s to %s' % (src, dst)
        print

    result_marker = ''
    q = LifoQueue(maxsize=5000)

    for i in xrange(args.threads_no):
        if args.verbose:
            print 'Adding worker thread %s for queue processing' % i
        t = Worker(q, i, aws_key, aws_secret_key,
                   src_bucket_name, dst_bucket_name,
                   src_path, dst_path, args)
        t.daemon = True
        t.start()

    i = 0

    while True:
        if args.verbose:
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
            if args.verbose:
                print 'Added %s keys to queue' % len(keys)
            if len(keys) < max_keys:
                if args.verbose:
                    print 'All items now in queue'
                break
            result_marker = keys[max_keys - 1].key
            while q.qsize() > (q.maxsize - max_keys):
                time.sleep(1)  # sleep if our queue is getting too big for the next set of keys
        except BaseException:
            logging.exception('error during fetch, quitting')
            break

    if args.verbose:
        print 'Waiting for queue to be completed'
    q.join()

    if args.verbose:
        print
        print 'Done'
        print


if __name__ == "__main__":

    j = os.path.join
    config_fn = j(os.environ['HOME'], '.s3cfg')

    parser = argparse.ArgumentParser(description = "s3 bucket to bucket copy")
    parser.add_argument('-c', '--config', 
                        help="s3 Configuration file. Defaults to ~/.s3cfg",
                        required = False,
                        default = config_fn)
    parser.add_argument('-v', '--verbose',
                        help="Verbose output",
                        required = False,
                        action = 'store_true',
                        default = False)
    parser.add_argument('-q', '--quiet',
                        help="No output",
                        required = False,
                        action = 'store_true',
                        default = False)
    parser.add_argument('-a', '--default-acl',
                        help = 'Skip copying ACL from src bucket and use the default ACL of the destination bucket instead',
                        dest='acl',
                        required = False,
                        action = 'store_true',
                        default = False)
    parser.add_argument('-e', '--encrypt',
                        help='Use server-side encryption to encrypt the file on S3',
                        required = False,
                        action = 'store_true',
                        default = False)
    parser.add_argument('-r', '--reduced-redundancy',
                        help='Store objects in the dest bucket using the reduced redundancy storage class',
                        dest='rr',
                        required = False,
                        action = 'store_true',
                        default = False),
    parser.add_argument('-t', '--thread-count',
                        help='Number of worker threads processing the objects',
                        dest='threads_no',
                        required = False,
                        type = int,
                        default = 20)
    parser.add_argument('src_bucket', 
                        help = "Source s3 bucket name and optional path")
    parser.add_argument('dest_bucket',
                        help = "Destination s3 bucket name")

    args = parser.parse_args()
      
    config = ConfigParser.ConfigParser()
    if not config.read(args.config):
        raise EnvironmentError("%s not found. Try running 's3cmd --configure'" % (args.config))

    default_aws_key = config.get('default', 'access_key')
    default_aws_secret_key = config.get('default', 'secret_key')

    if args.quiet:
        args.verbose = False

    copy_bucket(default_aws_key, default_aws_secret_key, args)
