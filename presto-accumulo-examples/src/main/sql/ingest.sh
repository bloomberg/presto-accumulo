#/bin/sh

if [[ $# -ne 1 ]]; then
    echo "Ingests data into the existing Presto table using presto-accumulo-tools 'batchwriter' utility"
    echo "Path is an HDFS file containing the TPC-H 'orders' dataset"
    echo "usage: sh src/main/sql/ingest.sh <orders.data.path>"
    exit 1
fi

WORKING_DIR="$( basename `pwd` )"
EXPECTED_DIR=presto-accumulo-examples

if [[ "$WORKING_DIR" != "$EXPECTED_DIR" ]]; then
    echo "Execute this script from presto-accumulo-examples directory"
    exit 1
fi

DATA=$1
JARFILE=`ls ../presto-accumulo-tools/target/presto-accumulo-tools-*.jar`
if [[ $? -ne 0 ]] ; then
    echo "Building presto-accumulo-tools..."
    cd ../presto-accumulo-tools/
    mvn clean install --quiet

    if [[ $? -ne 0 ]] ; then
        echo "Build failed!"
        exit 2
    fi

    cd ../presto-accumulo-examples
    JARFILE=`ls ../presto-accumulo-tools/target/presto-accumulo-tools-*.jar`
fi

echo "Ingesting data..."
hadoop jar $JARFILE batchwriter -d \| -f $DATA -s default -t orders

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest data"
    exit 2
fi
