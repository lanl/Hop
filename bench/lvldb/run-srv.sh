#!/bin/bash -x

MASTER=$1
ADDR=$2
DB=$3

LVLDBROOT=$HOME/work/leveldb
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LVLDBROOT

rm -rf $DB
mkdir -p $DB
echo $HOME/work/go/src/hop/lvldbhop/ldd2hop/ldd2hop -proto=tcp -addr=$ADDR -maddr=$MASTER -dbname=$DB > $HOME/tmp/server-$ADDR.log
$HOME/work/go/src/hop/lvldbhop/ldd2hop/ldd2hop -proto=tcp -addr=$ADDR -maddr=$MASTER -dbname=$DB 2>&1 | tee $HOME/tmp/server-$ADDR.log
