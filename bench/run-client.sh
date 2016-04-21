#!/bin/bash

OPNUM=1000000
THREADNUM=16
HOP=d2hop
MHOP=false
SEED=13
KEYNUM=65536
VMIN=24576
VMAX=32768
MASTER=$1
ADDR=$2
WAIT=$3
CHOPMASTER=$5
CHOPADDR=$6
CHOPDOMAIN=$7
RNDSEED=$(($SEED + $4))

echo /users/lionkov/work/go/src/hop/bench/bench -proto=tcp -N=$OPNUM -T=$WAIT -maddr=$MASTER -threadnum=$THREADNUM -vmin=$VMIN -vmax=$VMAX -S=$RNDSEED -knum=$KEYNUM -hop=$HOP -chopmaddr=$CHOPMASTER -chopaddr=$CHOPADDR -chopdomain=$CHOPDOMAIN > /users/lionkov/tmp/results-$ADDR.log
/users/lionkov/work/go/src/hop/bench/bench -proto=tcp -N=$OPNUM -T=$WAIT -maddr=$MASTER -threadnum=$THREADNUM -vmin=$VMIN -vmax=$VMAX -S=$RNDSEED -knum=$KEYNUM -hop=$HOP -chopmaddr=$CHOPMASTER -chopaddr=$CHOPADDR -chopdomain=$CHOPDOMAIN 2>&1 | tee -a /users/lionkov/tmp/results-$ADDR.log &
