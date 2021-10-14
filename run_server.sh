#!/bin/sh

# run memcached server at 127.0.0.1:11221
memcached -B ascii -U 0 -l 127.0.0.1 -p 11221
