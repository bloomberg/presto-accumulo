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

* [Index Migration](#index-migration)

## Dependencies
* Java 1.7
* Maven
* Accumulo 1.7 or greater
* Spark 2.x (for Index Migration)

## Usage
Build the `presto-accumulo-tools` jar file using Maven, then execute the jar file to see all available tools:

```bash
$ cd presto-accumulo-tools/
$ mvn clean package
$ java -jar target/presto-accumulo-tools-*.jar 
Usage: java -jar <jarfile> <tool> [args]
Execute java -jar <jarfile> <tool> --help to see help for a tool.
Available tools:
	index-migration	Copies the data of a Presto/Accumulo to a new table using the new indexing methods
	pagination	Queries a Presto table for rows of data, interactively displaying the results in pages
	query-metrics	Queries the metrics and trace tables for information regarding a Presto query
```

## Available Tools

### index-migration

This tool is used to migrate data from the previous Index 1.0 method to the new Index 2.0 strategies.

```bash
$ java -jar target/presto-accumulo-tools-0.184.0.bb-SNAPSHOT.jar index-migration --help
Missing required options: s, d
usage: usage: java -jar <jarfile> index-migration [args]
 -a,--auths <arg>            Authorization string. Note that you'll need
                             to specify all auths to cover all entries in
                             the table to be properly copied
 -c,--config <arg>           accumulo.properties file
 -d,--dest-table <arg>       Dest table to copy to
    --help                   Print this help message
 -n,--num-partitions <arg>   Number of partitions to create when writing
                             to Accumulo
 -s,--src-table <arg>        Source table to copy from
```

The workflow is:

Prereqs: Update your ingestion job to use the new version of `presto-accumulo`, 0.184.0.bb or later.  We will be stopping ingest and deploying this new version when ingest is re-enabled.

1. Stop ingest on the old tables
2. Rename the old table via SQL using the previous version of Presto
```sql
ALTER TABLE foo RENAME TO foo_old;
```
3. Launch the new version of Presto
4. Run the DDL to create the new (empty) tables
5. (Optional) Deploy the new ingestion job using the new version of `presto-accumulo`.  Ingest will begin writing to the old table, and we can migrate to the new table.
    1.  If able, you should wait to deploy the updated ingestion job until after the migration and validation is complete.  If something happens, you'll need to account for the data that has been written the new tables and not in the old tables on rollback.
6. Run the Spark job to copy the old table to the new table

```bash
spark-submit --master local[*] \
    --class com.facebook.presto.accumulo.tools.Main \
    ./target/presto-accumulo-tools-0.184.0.bb-SNAPSHOT.jar \
    index-migration \
    -u root \
    -p secret \
    -i default \
    -z localhost:2181 \
    -s foo_old \
    -d foo \
    -a admin \
    -n 100
```

7. Once it is complete, do some validation and you can then offline/delete the old Accumulo tables and deploy the new ingestion job if you haven't done so already.

Rollback instructions:

1. Alter the new table to give it a new name, or alternatively drop the table.  If dropping an external table, use the Accumulo shell to delete the Accumulo tables.
```sql
ALTER TABLE foo RENAME TO foo_new;
--or
DROP TABLE foo;
```

2. Deploy the previous version of Presto
3. Rename the old table to new
```
ALTER TABLE foo_old RENAME TO foo;
```

4. Turn on ingestion using the previous version of `presto-accumulo`
