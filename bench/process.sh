#!/bin/bash

echo '# Nodes Bandwidth Rate RequestSize'

for d in $1/* ; do
	N=`grep Bandwidth $d/results-* | awk '{sum += $2} END { print NR }'`
	BW=`grep Bandwidth $d/results-* | awk '{sum += $2} END { print sum }'`
	RT=`grep Rate $d/results-* | awk '{sum += $2} END { print sum }'`
	RSZ=`grep ReqSize $d/results-* | awk '{sum += $2} END { print sum / NR }'`
	echo $N $BW $RT $RSZ
done
