#!/bin/bash

DBDIR=/tmp/nvm/lionkov/kcdb
ETHP=192.168.0
IBP=10.142.0

DELAY=$(($2*2 + 60))
ALLNODES=`scontrol show hostname $SLURM_NODELIST`
NODES=`for i in $ALLNODES; do echo $i | sed -e 's/ta0//' -e 's/ta//'; done`
SRVNODES=`for i in $NODES; do echo $i; done | tail -$1`
CLTNODES=`for i in $NODES; do echo $i; done | head -$2`
MASTER=`for i in $SRVNODES; do echo $i; exit; done`
CLIENT=`for i in $CLTNODES; do echo $i; exit; done`

rm -f $HOME/tmp/results*.log $HOME/tmp/server*.log

# start cassandra
for nd in $SRVNODES; do
	echo Starting server on $nd
	ssh -f $ETHP.$nd $HOME/work/go/src/hop/bench/kcabinet/run-srv.sh $IBP.$MASTER:5000 $IBP.$nd:5000 $DBDIR/$nd/kdb.kch
	sleep 2
done

sleep 10

echo Starting benchmark...

i=0
for nd in $CLTNODES; do
	D=$(($DELAY - $i*2))
	echo Starting $nd after $D seconds
	ssh -f $ETHP.$nd $HOME/work/go/src/hop/bench/kcabinet/run-clt.sh $IBP.$nd:5000 $D $i $IBP.$MASTER:5000
	i=$((i + 1))
	sleep 2
done
