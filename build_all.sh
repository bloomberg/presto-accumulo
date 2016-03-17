#!/bin/sh

# Builds/installs all presto-accumulo projects

# Project build order

PROJECTS=( "presto-accumulo" "presto-accumulo-iterators" "presto-accumulo-benchmark" "presto-accumulo-tools" )

for PROJ in ${PROJECTS[@]}; do
    cd $PROJ
    mvn clean install -DskipTests
    if [[ $? -ne 0 ]]; then
        echo "Build of $PROJ failed"
        exit 1
    fi
    cd ../
done

