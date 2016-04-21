#!/bin/bash

MASTER=$1
ADDR=$2

$HOME/work/go/src/hop/d2hop/d2hopsrv/d2hopsrv -proto=tcp -addr=$ADDR -maddr=$MASTER &
