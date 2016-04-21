#!/bin/bash

DELAY=$(($2*2 + 60))
ALLNODES=`scontrol show hostname $SLURM_NODELIST`
SRVNODES=`for i in $ALLNODES; do echo $i-ib0; done | tail -$1`
CLTNODES=`for i in $ALLNODES; do echo $i-ib0; done | tail -$2`
#SRVNODES=`for i in $ALLNODES; do echo $i; done | head -$1`
#CLTNODES=`for i in $ALLNODES; do echo $i; done | head -$2`

# generate mcached.conf
echo --BINARY-PROTOCOL > mcached.conf
for i in $SRVNODES; do
	echo --SERVER=$i >> mcached.conf
done

# start memcached
for nd in $SRVNODES; do
	echo Start memcached on $nd
	ssh $nd /users/lionkov/bin/memcached -l $nd -R 10000 -m 8000 -t 16 -d -c 8192 -B binary
	sleep 1
done

sleep 10

echo Start benchmark
i=0
for nd in $CLTNODES; do
	D=$(($DELAY - $i*2))
	echo Starting $nd after $D seconds
	ssh -f $nd /users/lionkov/work/go/src/hop/bench/memc/run.sh $nd $D $i
	i=$((i + 1))
	sleep 2
done
