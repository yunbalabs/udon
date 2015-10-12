Yunba Redis Cluster
==========================

A Redis cluster solution based on [riak_core][0].

Quick Start
===========
Clone this project
```
git clone https://github.com/yunbalabs/udon.git udon
```

Compile and play around
=======================
```
$ cd udon
$ make rel
```

Start node
```
$ ./rel/udon/bin/udon start
```

Play with redis-cli
```
$ redis-cli -p 6380
127.0.0.1:6380> SADD 1,1 1
(integer) 1
127.0.0.1:6380> SMEMBERS 1,1
1) "1"
127.0.0.1:6380> SREM 1,1 1
(integer) 1
127.0.0.1:6380> SMEMBERS 1,1
(empty list or set)
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

Supported Commands
======
### SADD bucket_name,key member
```
sadd bucket_1,1 123
```

### SREM bucket_name,key member
```
srem bucket_1,1 123
```

### SMEMBERS bucket_name,key
```
smembers bucket_1,1
```

### EXPIRE bucket_name,key seconds
```
expire bucket_1,1 60
```

### STAT_APPKEY_ONLINE stat,key appkey uid seconds
```
stat_appkey_online stat,5562d79527302bb3158937d7_1 5562d79527302bb3158937d7 2449968497667150720 60
```

### STAT_APPKEY_OFFLINE stat,key appkey uid seconds
```
stat_appkey_offline stat,5562d79527302bb3158937d7_1 5562d79527302bb3158937d7 2449968497667150720 60
```

### STAT_APPKEY stat,appkey type date date_type range
```
stat_appkey stat,5562d79527302bb3158937d7 active 2015-09-08-16-54-07 d 3
```

Benchmarking
======
1. Download [basho_bench](https://github.com/basho/basho_bench).
2. Add [udon_client](https://github.com/yunbalabs/udon_client) to the basho_bench's deps.
3. Copy basho_bench_driver_eredis.erl to the src directory.
4. Copy basho_bench_eredis.config to the examples directory. Change the udon_instances in the deps/udon_client/src/udon_client_app.erl.
5. Compile and run:

    ```
    ./basho_bench examples/basho_bench_eredis.config
    ```
6. View the result `tests/current/summary.csv`.

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
