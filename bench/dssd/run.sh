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
DB=$4

cd $HOME/work/go/src/hop/bench/dssd
echo ./dssd -N $OPNUM -T $WAIT -c $DB -t $THREADNUM -m $VMIN -x $VMAX -S $RNDSEED -k $KEYNUM 2>&1 > $HOME/tmp/results-$ADDR.log
./dssd -N $OPNUM -T $WAIT -c $DB -t $THREADNUM -m $VMIN -x $VMAX -S $RNDSEED -k $KEYNUM 2>&1 | tee -a $HOME/tmp/results-$ADDR.log &
