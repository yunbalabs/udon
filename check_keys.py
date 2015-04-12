#!/usr/bin/env python

#
# Tools for devrel
#
# Functionality:
# 1. discovery the keys, nodes, vnodes relations
# 2. check if replicas of a key in different vnodes are identical
#

__author__ = 'zhanghu'

import os
import redis
import pprint
import argparse

SOCKFILEPATH="/tmp/"
PRIFIX="redis.sock.udon"
#
# data key in node,vnode
# bucket1,key1: node1,vnode1; node2,vnode2
#
KEYS = {}

# a key residents in which nodes?
KEY_NODES = {}

#
# a node has which vnodes?
#
NODE_VNODES = {}

# a node has which keys?
NODE_KEYS = {}

#
# nodes resident in which nodes
# vnode1: node1; node2
#
VNODES = {} # vnode in nodes

def get_sock_files():
    return os.listdir(SOCKFILEPATH)

def get_node_vnode(unixsock):
    [n1, n2] = unixsock.split("@")
    return [n1.split(".")[-1], n2.split(".")[-1]]

def get_redis_keys(unixsock):
    r = redis.Redis(unix_socket_path=unixsock)
    return r.keys("*")

def append(map, key, val):
    if not map.has_key(key):
        map[key] = []
    map[key].append(val)

# /tmp/redis.sock.udon1@127.0.0.1.1073290264914881830555831049026020342559825461248
def sock_filename(node, vnode):
    return SOCKFILEPATH + "redis.sock." + node + "@127.0.0.1." + vnode
#
# check if the replicas of a key in diffrent vnodes are identical
#
def check_keys_in_vnodes():
    pprint.pprint("## check replica of keys")
    global KEYS

    result = {}
    for key in KEYS:
        pprint.pprint(key)
        key_val = None
        for node_vnode in KEYS[key]:
            sockfile = sock_filename(node_vnode[0], node_vnode[1])
            # pprint.pprint(sockfile)
            r = redis.Redis(unix_socket_path = sockfile)
            val = r.smembers(key)

            if key_val == None:
                key_val = val
                pprint.pprint(node_vnode)
                result[key] = "No replica"
            else:
                if not key_val == val:
                    pprint.pprint("replicas of key", key, "are not identical")
                    result[key] = "Failed"
                else:
                    pprint.pprint([node_vnode, "OK"])
                    result[key] = "OK"

    pprint.pprint(result)

parser = argparse.ArgumentParser(description='')
parser.add_argument('--type', dest='checktype', required = True,
                    help='check types: keys, keys_simple, nodes, vnodes, key_replica, all')

args = parser.parse_args()
# print args.checktype

def main():
    global KEYS, KEY_NODES, NODE_VNODES, NODE_KEYS, VNODES
    sockfiles = get_sock_files()
    for sockfile in sockfiles:
        if sockfile.startswith(PRIFIX):
            sockfile_path = SOCKFILEPATH + sockfile
            [node, vnode] = get_node_vnode(sockfile_path)
            for key in get_redis_keys(sockfile_path):
                append(KEYS, key, [node, vnode])
                append(KEY_NODES, key, node)
                append(VNODES, vnode, node)
                append(NODE_VNODES, node, vnode)
                append(NODE_KEYS, node, key)

    if args.checktype == 'all' or args.checktype == 'keys':
        pprint.pprint(KEY_NODES)
        pprint.pprint(KEYS)
    if args.checktype == 'keys_simple':
       pprint.pprint(KEY_NODES)
    if args.checktype == 'all' or args.checktype == 'nodes':
        pprint.pprint(NODE_VNODES)
        pprint.pprint(NODE_KEYS)
    if args.checktype == 'all' or args.checktype == 'vnodes':
        pprint.pprint(VNODES)
    if args.checktype == 'all' or args.checktype == 'key_replica':
        check_keys_in_vnodes()

if __name__ == '__main__':
    main()

