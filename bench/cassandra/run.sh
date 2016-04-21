#!/bin/bash

OPNUM=10000
THREADNUM=16
SEED=13
KEYNUM=32768
VMIN=262144
VMAX=1048576
ADDR=$1
WAIT=$2
RNDSEED=$(($SEED + $3))
MASTER=$4
LD_LIBRARY_PATH=$HOME/cassandra/lib64:$HOME/cassandra/lib:$HOME/libuv/lib
export LD_LIBRARY_PATH

cd $HOME/work/go/src/hop/bench/cassandra/
echo ./csbench -N $OPNUM -T $WAIT -c $MASTER -t $THREADNUM -m $VMIN -x $VMAX -S $RNDSEED -k $KEYNUM 2>&1 > $HOME/tmp/results-$ADDR.log
./csbench -N $OPNUM -T $WAIT -c $MASTER -t $THREADNUM -m $VMIN -x $VMAX -S $RNDSEED -k $KEYNUM 2>&1 | tee -a $HOME/tmp/results-$ADDR.log &
