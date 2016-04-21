#!/bin/bash

NDELAY=5
DELAY=$(($1*$NDELAY + 120))
#DELAY=$(($1*2 + 60))
NODES=`scontrol show hostname $SLURM_NODELIST | head -$1`
IBNODES=`for i in $NODES; do echo $i-ib0; done`
MASTER=`for i in $IBNODES; do echo $i; exit; done`

i=0
for nd in $IBNODES; do
	D=$(($DELAY - $i*$NDELAY))
	echo Starting $nd after $D seconds
	ssh -f $nd /users/lionkov/work/go/src/hop/bench/run.sh $MASTER:5000 $nd:5000 $D $i $MASTER:6000 $nd:6000 $(($i / 8))
	i=$(($i + 1))
	sleep $NDELAY
done
