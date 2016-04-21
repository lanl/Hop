#!/bin/bash

DELAY=$(($1 + 30))
NSRV=$(($1 / 4))

for i in `seq 1 $NSRV`; do
	D=$(($DELAY - $i))
	echo Starting server $i after $D seconds
	ssh -f 10.10.4.$i /users/lionkov/work/go/src/hop/d2hop/bench/run.sh 10.10.4.1:5000 10.10.4.$i:5000 $D $i
	sleep 1
done

for i in `seq $(($NSRV + 1)) $1`; do
	D=$(($DELAY - $i))
	echo Starting client $i after $D seconds
	ssh -f 10.10.4.$i /users/lionkov/work/go/src/hop/d2hop/bench/run-client.sh 10.10.4.1:5000 10.10.4.$i:5000 $D $i
	sleep 1
done

