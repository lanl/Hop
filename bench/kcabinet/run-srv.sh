#!/bin/bash

MASTER=$1
ADDR=$2
DB=$3

KCROOT=$HOME/kcabinet
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$KCROOT/lib

rm -f $DB
mkdir -p `dirname $DB`
echo $HOME/work/go/src/hop/kchop/kcd2hop/kcd2hop -proto=tcp -addr=$ADDR -maddr=$MASTER -dbname=$DB > $HOME/tmp/server-$ADDR.log
$HOME/work/go/src/hop/kchop/kcd2hop/kcd2hop -proto=tcp -addr=$ADDR -maddr=$MASTER -dbname=$DB 2>&1 | tee $HOME/tmp/server-$ADDR.log

