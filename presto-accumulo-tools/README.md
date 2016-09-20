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

* [Rewrite metrics](#rewritemetrics)
* [Timestamp check](#timestamp-check)

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
