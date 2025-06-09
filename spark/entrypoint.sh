#!/bin/bash

start-master.sh -p 7077
start-worker.sh spark://spark-iceberg:7077
start-history-server.sh
start-thriftserver.sh --driver-java-options "-Dderby.system.home=/tmp/derby"

JAR_DIR="/opt/spark/jars"

# Create the directory if it doesn't exist
mkdir -p $JAR_DIR

# Download required JARs
wget -P $JAR_DIR https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
wget -P $JAR_DIR https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

# Set Spark classpath
export SPARK_CLASSPATH="$JAR_DIR/*"

if [[ $# -gt 0 ]] ; then
    eval "$1"
fi