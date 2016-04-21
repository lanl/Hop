#!/bin/bash

OPNUM=100000
THREADNUM=16
MASTER=$1
ADDR=$2
WAIT=$3
RNDSEED=$4

/users/lionkov/work/go/src/hop/d2hop/bench/bench -proto=ib -N=$OPNUM -T=$WAIT -maddr=$MASTER -threadnum=$THREADNUM -vmin=4 -vmax=65536 -S=$RNDSEED | tee /users/lionkov/tmp/results-$ADDR.log &

#for i in 1 ; do
#	/users/lionkov/work/go/src/hop/d2hop/bench/bench -proto=ib -N=$OPNUM -T=$WAIT -maddr=$MASTER -threadnum=$THREADNUM -vmin=4 -vmax=65536 | tee /users/lionkov/tmp/results-$ADDR-$i.log &
#done
