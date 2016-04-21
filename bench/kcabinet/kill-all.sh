#!/bin/bash

export DATABASE=/tmp/nvm/lionkov/kcdb
for i in `scontrol show hostname $SLURM_NODELIST`; do
	ssh $i "rm -rf $DATABASE; killall kcd2hop ; killall bench"
done
