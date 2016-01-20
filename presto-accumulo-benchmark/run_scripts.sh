#!/bin/sh

SERVER=localhost:8080
CATALOG=accumulo
SCHEMA=default

SCRIPTS_DIR=scripts
BLACKLIST=("counts.sql" "create_tables.sql" "drop_tables.sql" "2.sql" "4.sql" "11.sql" "13.sql" "15.sql" "17.sql" "20.sql" "21.sql" "22.sql")

# Some TPC-H queries are blacklisted for the following reasons:

# 2.sql -- scalar subqueries are not supported 
# 4.sql -- EXISTS predicate is not yet implemented
# 11.sql -- scalar subqueries are not supported
# 13.sql -- Non-equi joins not supported
# 15.sql -- Connector does not support creating views : yet?
# 17.sql -- scalar subqueries are not supported
# 20.sql -- scalar subqueries are not supported
# 21.sql -- EXISTS predicate is not yet implemented
# 22.sql -- scalar subqueries are not supported

# Relevant issues to the above queries:

# 2/4/11/17/20/21/22:
# https://github.com/facebook/presto/issues/2878
# https://github.com/facebook/presto/issues/3902
# https://github.com/facebook/presto/issues/4017

# 13
# https://github.com/facebook/presto/issues/3888

for FILE in `ls $SCRIPTS_DIR | sort -n`; do
    if [[ " ${BLACKLIST[@]} " =~ " ${FILE} " ]]; then
        echo "Skipping $FILE"
    else
        echo "Executing $FILE"
        cat $SCRIPTS_DIR/$FILE
        echo ; echo
        time presto --server $SERVER --catalog $CATALOG --schema $SCHEMA -f $SCRIPTS_DIR/$FILE
        echo
    fi
done

