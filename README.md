# Overview
This is the middleware source code for the ETH course [Advanced Systems Lab](https://archive-systems.ethz.ch/courses/fall2017/asl) (fall semester 2017).
The goal of this course was to develop a middleware between load generating Memtier clients and Memcached servers and perform an extensive system analysis when running different workloads.

# Prerequisites
- Linux
- Java 8+
- Ant
- [Memcached](https://github.com/memcached/memcached/wiki/Install)
- [Memtier](https://github.com/RedisLabs/memtier_benchmark)

# Get started
- Run server: `run_server.sh`
- Compile and run middleware: `run_mw.sh`
- Run client (script requires parameters): `run_client.sh`