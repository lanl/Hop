#!/bin/bash

export DATABASE=/tmp/nvm/lionkov/ldb
for i in `scontrol show hostname $SLURM_NODELIST`; do
	ssh $i "rm -rf $DATABASE; killall ldd2hop ; killall bench"
done
