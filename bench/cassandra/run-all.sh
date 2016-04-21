#!/bin/bash

CSROOT=$HOME/work/apache-cassandra-2.1.4
DATABASE=/tmp/nvm/lionkov/cassandra
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
	echo Start cassandra on $nd
	ssh $ETHP.$nd $HOME/work/go/src/hop/bench/cassandra/run-server.sh $IBP.$MASTER $DATABASE/$nd
#	sleep 5
done

echo Waiting for servers to stabilize
sleep 60

echo Start benchmark
ssh $ETHP.$CLIENT $HOME/work/go/src/hop/bench/cassandra/setup.sh $IBP.$MASTER

i=0
for nd in $CLTNODES; do
	D=$(($DELAY - $i*2))
	echo Starting $nd after $D seconds
	ssh -f $ETHP.$nd $HOME/work/go/src/hop/bench/cassandra/run.sh $IBP.$nd $D $i $IBP.$MASTER
	i=$((i + 1))
	sleep 2
done
