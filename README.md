Yunba Redis Cluster
==========================

A Redis cluster solution based on [riak_core][0].

Quick Start
===========
Clone this project
```
git clone https://github.com/TigerZhang/udon.git udon
```

Compile and play around
=======================
```
$ cd udon
$ make devrel
```

Start dev nodes
```
$ ./devall.sh start
```

Create the cluster
```
$ . ./alias.sh
$ u2a cluster join 'udon1@127.0.0.1'
$ u3a cluster join 'udon1@127.0.0.1'
$ u4a cluster join 'udon1@127.0.0.1'
$ u1a cluster plan
$ u1a cluster commit
```

Add some datas
```
./add_test.erl 10
```

Check the keys/nodes/vnodes
```
$ ./check_keys.py --help
usage: check_keys.py [-h] --type CHECKTYPE

optional arguments:
  -h, --help        show this help message and exit
  --type CHECKTYPE  check types: keys, keys_simple, nodes, vnodes,
                    key_replica, all
```

Caution
======
1. Just sadd/srem supported
2. It's just a early demo, not suitable for product.

Todo
====
 * Tidy source codes
 * Monitor redis-server status in vnode
 * Integrate riak_control

This project is modified based on the source codes of https://github.com/mrallen1/udon. The file based storage is removed.

udon: a distributed static file web server
=============================

Udon is a static file web service, generally intended to be used at the edge of a network to serve static assets for websites like CSS files, javascript, images or other content which does not change frequently or is dynamically part of the web application state.

It's also built on top of [riak_core][0] and provides a simple application which is intended to introduce the kinds of programming that's required to implement a riak_core application including handoff logic between vnodes.  I wrote this as a tool to teach for [Erlang Factory SF 2015][1].

Slides and video
----------------
The slides can be found [on SpeakerDeck][2].

The video has not been posted yet, but I will update this README when it has.

[0]: https://github.com/basho/riak_core
[1]: http://www.erlang-factory.com/sfbay2015/mark-allen
[2]: https://speakerdeck.com/mrallen1/building-distributed-applications-with-riak-core 
