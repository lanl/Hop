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
LD_LIBRARY_PATH=$HOME/kcabinet/lib
export LD_LIBRARY_PATH

cd $HOME/work/go/src/hop/bench
echo ./bench proto=tcp -N=$OPNUM -T=$WAIT -maddr=$MASTER -threadnum=$THREADNUM -vmin=$VMIN -vmax=$VMAX -S=$RNDSEED -knum=$KEYNUM -hop=d2hop > $HOME/tmp/results-$ADDR.log
./bench -proto=tcp -N=$OPNUM -T=$WAIT -maddr=$MASTER -threadnum=$THREADNUM -vmin=$VMIN -vmax=$VMAX -S=$RNDSEED -knum=$KEYNUM -hop=d2hop 2>&1 | tee -a $HOME/tmp/results-$ADDR.log &
