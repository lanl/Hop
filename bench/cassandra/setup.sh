#!/bin/bash

ADDR=$1
LD_LIBRARY_PATH=$HOME/cassandra/lib64:$HOME/cassandra/lib:$HOME/libuv/lib
export LD_LIBRARY_PATH

cd $HOME/work/go/src/hop/bench/cassandra/
./csbench -s -c $ADDR
