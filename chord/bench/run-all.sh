#!/bin/bash

DELAY=$(($1*4 + 120))

for i in `seq 1 $1`; do
	D=$(($DELAY - $i*2))
	echo Starting $i after $D seconds
	ssh -f 10.10.4.$i /users/lionkov/work/go/src/hop/chord/bench/run.sh 10.10.4.1:5000 10.10.4.$i:5000 $D $i
	sleep 2
done
