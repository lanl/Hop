#!/bin/bash

export DATABASE=/tmp/nvm/lionkov/cassandra
for i in `scontrol show hostname $SLURM_NODELIST`; do
	ssh $i "rm -rf $DATABASE; killall java ; killall csbench"
done
