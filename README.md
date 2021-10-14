# Overview
This is the middleware source code for the ETH course [Advanced Systems Lab](https://archive-systems.ethz.ch/courses/fall2017/asl) (fall semester 2017).
The goal of this course was to develop a middleware between load generating Memtier clients and Memcached servers and perform an extensive system analysis when running different workloads.

# Prerequisites
- Linux
- Java 8+
- Ant
- [Memecached](https://github.com/memcached/memcached/wiki/Install)

# Get started
Compile/run middleware (and adjust parameters): `run_mw.sh`

or

Compile with ant:  `ant -f build.xml`