<!---
Copyright 2016 Bloomberg L.P.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Presto/Accumulo Tools

A collection of tools to assist in tasks related to the Accumulo Connector for Presto

* [Rewrite index](#rewriteindex)
* [Rewrite metrics](#rewritemetrics)
* [Timestamp check](#timestamp-check)

## Note
This is a forked branch that provides support for Accumulo 2. In order to build you will need to pull and build the accumulo2 branch of presto https://github.com/cch-k/presto/tree/master/presto-accumulo
From the presto-accumulo2 directory run `mvn install -am -B -V -T C1 -DskipTests -Dair.check.skip-all -U -Dmaven.javadoc.skip=true`
Make sure the accumulo2 version in this pom.xml match the version of the accumulo2 jar that was built

## Dependencies
* Java 1.7
* Maven
* Accumulo
* _presto-accumulo_ (Built and installed from this repository)

## Usage
Build the `presto-accumulo-tools` jar file using Maven, then execute the jar file to see all available tools:

```bash
$ cd presto-accumulo-tools/
$ mvn clean package
$ java -jar target/presto-accumulo-tools-*.jar 
Usage: java -jar <jarfile> <tool> [args]
Execute java -jar <jarfile> <tool> --help to see help for a tool.
Available tools:
    pagination  Queries a Presto table for rows of data, interactively displaying the results in pages
    rewriteindex    Re-writes the index and metrics table based on the data table
    rewritemetrics  Re-writes the metrics table based on the index table
```

## Available Tools

### rewriteindex
Rewrites the index and metrics for a given Presto table.  Use this tool if:

1. You change an existing column's index status

While newly inserted rows, either using SQL or the `PrestoBatchWriter`, will contain new index and metric entries, existing rows of data will not be indexed and will not show up in the result set of a query that uses that column's index.  Run this tool to rebuild the index and correct your table.

2. Regularly insert rows containing the same row ID

Inserting a row with the same row ID, either using SQL or the `PrestoBatchWriter`, will cause the metric counts to be larger than the actual number.  You can run this tool, but if the index entries do not need to be rebuilt, you can simply run the [rewriteindex|.

3. Manually delete data records without modifying the index or metrics

Deleting rows from the data table is fine, but the entries in the index and metrics table would not accurately reflect the table.  Queries will still be correct, but the planning time will be inflated due to gathering extra index rows that are never used, and the optimizer could make a poor decision because of inaccurate metrics.

__*Example Usage*__

Running the below command without the `--force` flag will do a dry-run of the tool, making no changes to the underlying tables but printing metrics about what would have been changed.

```bash
java -jar target/presto-accumulo-tools-*.jar rewriteindex \
-c /path/to/presto/etc/catalog/accumulo.properties -s default -t foo -a private
```
If you're happy with what you're seeing, run again with the `--force` flag to make the changes.
```bash
java -jar target/presto-accumulo-tools-*.jar rewriteindex \
-c /path/to/presto/etc/catalog/accumulo.properties -s default -t foo -a private --force
```

### rewritemetrics 
Rewrites the metrics for a given Presto table.  Use this tool if you regularly insert rows containing the same row ID.  This will read the index table and update the metrics table with the correct values.

__*Example Usage*__

Running the below command without the `--force` flag will do a dry-run of the tool, making no changes to the underlying table but printing metrics about what would have been changed.

```bash
java -jar target/presto-accumulo-tools-*.jar rewritemetrics \
-c /path/to/presto/etc/catalog/accumulo.properties -s default -t foo -a private
```
If you're happy with what you're seeing, run again with the `--force` flag to make the changes.
```bash
java -jar target/presto-accumulo-tools-*.jar rewritemetrics \
-c /path/to/presto/etc/catalog/accumulo.properties -s default -t foo -a private --force
```

### timestamp-check
Scans the metrics, index, and data tables for the number of entries in a timespan.  Used to help diagnose issues regarding differences in the three tables for timestamp-based columns.

__*Example Usage*__


```bash
java -jar target/presto-accumulo-tools-*.jar timestamp-check \
-c /path/to/presto/etc/catalog/accumulo.properties -s default -t foo -a private \
-col recordtime -st 2016-08-01T00:00:00.000+0000 -e 2016-09-01T00:00:00.000+0000

2016-09-20 12:23:23,804 [pool-5-thread-1] INFO  tools.TimestampCheckTask: Getting data count
2016-09-20 12:23:23,805 [pool-5-thread-2] INFO  tools.TimestampCheckTask: Getting index count
2016-09-20 12:23:23,806 [pool-5-thread-3] INFO  tools.TimestampCheckTask: Getting metric count
2016-09-20 12:23:24,313 [pool-5-thread-2] INFO  tools.TimestampCheckTask: Number of rows in index table is 25764
2016-09-20 12:23:24,356 [pool-5-thread-2] INFO  tools.TimestampCheckTask: Number of index ranges is 25764
2016-09-20 12:23:24,369 [pool-5-thread-2] INFO  tools.TimestampCheckTask: Number of distinct index ranges is 25764
2016-09-20 12:23:24,570 [pool-5-thread-3] INFO  tools.TimestampCheckTask: Number of rows from metrics table is 25764
2016-09-20 12:23:26,877 [pool-5-thread-2] INFO  tools.TimestampCheckTask: Number of rows from data table via index is 25764
2016-09-20 12:23:26,877 [pool-5-thread-2] INFO  tools.TimestampCheckTask: Number of rows from data table outside the time range is 0
2016-09-20 12:23:26,877 [pool-5-thread-2] INFO  tools.TimestampCheckTask: Number of rows in the index not scanned from the table is 0
2016-09-20 12:23:27,884 [pool-5-thread-1] INFO  tools.TimestampCheckTask: Number of rows from data table is 25764
```
