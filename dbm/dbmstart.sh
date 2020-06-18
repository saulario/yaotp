#!/bin/bash

DBMANAGER_VERSION="1.0"
DBMANAGER_HOME=${HOME}/tdi/dbmanager-${DBMANAGER_VERSION}
DBMANAGER_CONF=${DBMANAGER_HOME}/conf/$1

for file in `ls $DBMANAGER_CONF`; do
    nohup ./reader.py --config=`basename $file` >/dev/null 2>&1 &
    nohup ./decoder.py --config=`basename $file` >/dev/null 2>&1 &
done