#!/bin/sh

# Builds/installs all presto-accumulo projects

# Project build order
PROJECTS=( "presto-accumulo-iterators" "presto" "presto-accumulo-benchmark" "presto-accumulo-tools" "presto-accumulo-examples" )

for PROJ in ${PROJECTS[@]}; do
    cd $PROJ
    mvn -T 1C clean install -DskipTests
    if [[ $? -ne 0 ]]; then
        echo "Build of $PROJ failed"
        exit 1
    fi
    cd ../
done

