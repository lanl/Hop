#!/bin/bash

DB=kvbench
ETHP=192.168.0
IBP=10.142.0

DELAY=$(($1*2 + 60))
ALLNODES=`scontrol show hostname $SLURM_NODELIST`
NODES=`for i in $ALLNODES; do echo $i | sed -e 's/ta0//' -e 's/ta//'; done`
CLTNODES=`for i in $NODES; do echo $i; done | head -$1`
CLIENT=`for i in $CLTNODES; do echo $i; exit; done`

echo Start benchmark

ssh $ETHP.$CLIENT /opt/dssd/bin/flood rm $DB
i=0
for nd in $CLTNODES; do
	D=$(($DELAY - $i*2))
	echo Starting $nd after $D seconds
	ssh -f $ETHP.$nd $HOME/work/go/src/hop/bench/dssd/run.sh $IBP.$nd $D $i $DB
	i=$((i + 1))
	sleep 2
done
