#!/bin/bash

MASTER=$1
DATADIR=$2

export PATH=$HOME/jre1.8.0_60/bin:$PATH
CSROOT=$HOME/work/apache-cassandra-2.1.4
export CASSANDRA_CONF=/tmp/cassandra/conf

rm -rf $CASSANDRA_CONF
mkdir -p $CASSANDRA_CONF
cp -a $CSROOT/conf/* $CASSANDRA_CONF
cat $HOME/work/go/src/hop/bench/cassandra/cassandra.yaml | sed -e "s/seeds:.*/seeds: \"$MASTER\"/" -e "s|@DATADIR|$DATADIR|g" > $CASSANDRA_CONF/cassandra.yaml
rm -rf $DATADIR
mkdir -p $DATADIR
mkdir -p /tmp/nvm/lionkov/cassandra-logs

$HOME/work/apache-cassandra-2.1.4/bin/cassandra 2>&1 >/dev/null
