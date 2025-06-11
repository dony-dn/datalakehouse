#!/bin/bash

start-master.sh -p 7077
start-worker.sh spark://spark-iceberg:7077
start-history-server.sh
start-thriftserver.sh --driver-java-options "-Dderby.system.home=/tmp/derby"

if [[ $# -gt 0 ]] ; then
    eval "$1"
fi