#!/bin/bash

DELAY=$(($1 + 30))

for i in `seq 1 $1`; do
	D=$(($DELAY - $i))
	echo Starting $i after $D seconds
	ssh -f 10.10.4.$i /users/lionkov/work/go/src/hop/d2hop/bench/run.sh 10.10.4.1:5000 10.10.4.$i:5000 $D $i
	usleep 1000000
done
