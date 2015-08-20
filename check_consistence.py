#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ./check_exists.py --appkey 551103ba5cbaf552028da467 --src_host localhost --src_port 6379 --dest_host localhost --dest_port 6380

import sys
import redis
import time
import argparse

parser = argparse.ArgumentParser(description='')
parser.add_argument('--appkey', dest='appkey', required = True, help='yunba appkey')
parser.add_argument('--src_host', dest='src_host', required = True, help='source redis host')
parser.add_argument('--src_port', dest='src_port', required = True, help='source redis port')
parser.add_argument('--dest_host', dest='dest_host', required = True, help='udon redis host')
parser.add_argument('--dest_port', dest='dest_port', required = True, help='udon redis port')

args = parser.parse_args()

APPKEY = args.appkey
REDIS_HOST = args.src_host
REDIS_PORT = args.src_port
UDON_REDIS_HOST = args.dest_host
UDON_REDIS_PORT = args.dest_port

client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
udon_client = redis.Redis(host=UDON_REDIS_HOST, port=UDON_REDIS_PORT)

localtime = time.localtime(time.time())
now = time.strftime("%Y-%m-%d", localtime)

key = 'active_' + APPKEY + '_' + now
udon_key = 'stat,' + key

values = client.smembers(key)
udon_values = udon_client.smembers(udon_key)

common_values = values & udon_values
if common_values == values:
	print '%s have the same value.' % (key)
	sys.exit(0)
else:
	print '%s have not the same value. value:%s udon value:%s' % (key, values, udon_values)
	sys.exit(1)