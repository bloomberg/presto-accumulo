#!/usr/bin/env bash

if [[ $# -ne 1 ]]; then
    echo "Ingests data into the existing Presto table using presto-accumulo-tools 'batchwriter' utility"
    echo "Path is an HDFS file containing the TPC-H data sets"
    echo "usage: sh src/main/sql/ingest.sh <tpch.data.dir>"
    exit 1
fi

WORKING_DIR="$( basename `pwd` )"
EXPECTED_DIR=presto-accumulo-examples

if [[ "$WORKING_DIR" != "$EXPECTED_DIR" ]]; then
    echo "Execute this script from presto-accumulo-examples directory"
    exit 1
fi

DIR=$1
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

echo "Ingesting TPC-H data sets..."
java -jar $JARFILE batchwriter -d \| -f $DIR/customer.tbl -s default -t customer

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest customer data"
    exit 2
fi

java -jar $JARFILE batchwriter -d \| -f $DIR/lineitem_with_uuid.tbl -s default -t lineitem

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest lineitem data"
    exit 2
fi

java -jar $JARFILE batchwriter -d \| -f $DIR/nation.tbl -s default -t nation

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest nation data"
    exit 2
fi

java -jar $JARFILE batchwriter -d \| -f $DIR/orders.tbl -s default -t orders

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest orders data"
    exit 2
fi

java -jar $JARFILE batchwriter -d \| -f $DIR/part.tbl -s default -t part

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest part data"
    exit 2
fi

java -jar $JARFILE batchwriter -d \| -f $DIR/partsupp_with_uuid.tbl -s default -t partsupp

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest partsupp data"
    exit 2
fi

java -jar $JARFILE batchwriter -d \| -f $DIR/region.tbl -s default -t region

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest region data"
    exit 2
fi

java -jar $JARFILE batchwriter -d \| -f $DIR/supplier.tbl -s default -t supplier

if [[ $? -ne 0 ]] ; then
    echo "Failed to ingest supplier data"
    exit 2
fi

