#!/bin/bash

OPNUM=100000
THREADNUM=16
SEED=13
KEYNUM=32768
VMIN=12288
VMAX=16384
ADDR=$1
WAIT=$2
RNDSEED=$(($SEED + $3))

cd /users/lionkov/work/go/src/hop/bench/zht/
echo ./zhtb -z zht.conf -n neighbor.conf -N $OPNUM -T $WAIT -t $THREADNUM -m $VMIN -x $VMAX -S $RNDSEED -k $KEYNUM 2>&1 > /users/lionkov/tmp/results-$ADDR.log
./zhtb -z zht.conf -n neighbor.conf -N $OPNUM -T $WAIT -t $THREADNUM -m $VMIN -x $VMAX -S $RNDSEED -k $KEYNUM 2>&1 | tee -a /users/lionkov/tmp/results-$ADDR.log &
