#!/bin/bash

DELAY=$(($2 + 10))
ALLNODES=`scontrol show hostname $SLURM_NODELIST`
#SRVNODES=`for i in $ALLNODES; do echo $i-ib0; done | tail -$1`
#CLTNODES=`for i in $ALLNODES; do echo $i-ib0; done | tail -$2`
SRVNODES=`for i in $ALLNODES; do echo $i; done | tail -$1`
CLTNODES=`for i in $ALLNODES; do echo $i; done | tail -$2`

# generate neighbor.conf
rm -f neighbor.conf
for i in $SRVNODES; do
	echo $i-ib0 50000 >> neighbor.conf
done

# start zht server
echo Start zht server
for nd in $SRVNODES; do
	echo Start zht on $nd
	ssh -f $nd-ib0 /users/lionkov/work/go/src/hop/bench/zht/run-server.sh
	sleep 1
done

sleep 10

echo Start benchmark
i=0
for nd in $CLTNODES; do
	D=$(($DELAY - $i))
	echo Starting $nd after $D seconds
	ssh -f $nd /users/lionkov/work/go/src/hop/bench/zht/run.sh $nd-ib0 $D $i
	i=$((i + 1))
	sleep 1
done
