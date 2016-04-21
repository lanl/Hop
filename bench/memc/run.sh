#!/bin/bash

OPNUM=1000000
THREADNUM=16
SEED=13
KEYNUM=65536
VMIN=24576
VMAX=32768
ADDR=$1
WAIT=$2
RNDSEED=$(($SEED + $3))
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/users/lionkov/memcached/lib
export LD_LIBRARY_PATH

cd /users/lionkov/work/go/src/hop/bench/memc/
echo ./memc -N $OPNUM -T $WAIT -c mcached.conf -t $THREADNUM -m $VMIN -x $VMAX -S $RNDSEED -k $KEYNUM 2>&1 > /users/lionkov/tmp/results-$ADDR.log
./memc -N $OPNUM -T $WAIT -c mcached.conf -t $THREADNUM -m $VMIN -x $VMAX -S $RNDSEED -k $KEYNUM 2>&1 | tee -a /users/lionkov/tmp/results-$ADDR.log &
