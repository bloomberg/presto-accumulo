#!/bin/sh

TEMPLATE_DIR=templates
SCRIPT_DIR=scripts

mkdir -p $SCRIPT_DIR

SCHEMAS=("sf1" "sf100" "sf1000" "sf10000" "sf1000000" "sf300" "sf3000" "sf430000" "tiny")

NEW_SCHEMA=$1

if [[ " ${SCHEMAS[@]} " =~ " ${NEW_SCHEMA} " ]]; then
    for FILE in `ls templates`; do
        sed s/\${SCHEMA}/$1/g $TEMPLATE_DIR/$FILE > $SCRIPT_DIR/$FILE
    done
else
    echo "Schema must be one of ${SCHEMAS[@]}"
    exit 1
fi

