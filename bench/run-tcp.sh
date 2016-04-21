#!/bin/bash

#DELAY=$(($1*4 + 120))
DELAY=$(($2*2 + 60))
ALLNODES=`scontrol show hostname $SLURM_NODELIST`
#SRVNODES=`for i in $ALLNODES; do echo $i-ib0; done | tail -$1`
#CLTNODES=`for i in $ALLNODES; do echo $i-ib0; done | tail -$2`
SRVNODES=`for i in $ALLNODES; do echo $i; done | tail -$1`
CLTNODES=`for i in $ALLNODES; do echo $i; done | tail -$2`
MASTER=`for i in $SRVNODES; do echo $i; exit; done`

i=0
for nd in $SRVNODES; do
	D=$(($DELAY - $i*2))
	echo Starting server on $nd
	ssh -f $nd $HOME/work/go/src/hop/bench/run-srv.sh $MASTER-ib0:5000 $nd-ib0:5000
	i=$(($i + 1))
	sleep 2
done

i=0
for nd in $CLTNODES; do
	D=$(($DELAY - $i*2))
        echo Starting $nd after $D seconds
        ssh -f $nd $HOME/work/go/src/hop/bench/run-client.sh $MASTER-ib0:5000 $nd-ib0:5000 $D $i $MASTER:6000 $nd:6000 $(($i / 8))
        i=$(($i + 1))
        sleep 2
done
