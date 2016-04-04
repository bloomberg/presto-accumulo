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

# Presto Accumulo!

A Presto connector for reading and writing data backed by Apache Accumulo.
### Table of Contents
1. [Repository Contents](#repository-contents)
2. [Dependencies](#dependencies)
3. [Installing the Patched Presto](#installing-the-patched-presto)
4. [Installing the Iterator Dependency](#installing-the-iterator-dependency)
5. [Connector Configuration](#connector-configuration)
6. [Unsupported Features](#unsupported-features)
7. [Usage](#usage)
8. [External Tables](#external-tables)
9. [Secondary Indexing](#secondary-indexing)
10. [Session Properties](#session-properties)
11. [Serializers](#serializers)
12. [Metadata Management](#metadata-management)
13. [Adding Columns Management](#metadata-management)

### Repository Contents
This repository contains five sub-projects:

1. _presto_ - A patched version of Presto 0.142 containing support for the ANY clause.  This is similar to the __contains__ UDF that can be used to check if an element is in an array.  Users can use this clause instead of contains to enable predicate pushdown support -- and therefore the secondary index capability of the connector.  This folder also contains the `presto-accumulo` connector code.
2. _presto-accumulo-iterators_ - A collection of Accumulo iterators to be installed on the TabletServers.  These iterators are required to user the connector.
3. _presto-accumulo-benchmark_ - An implementation of the TPC-H benchmarking suite for testing the connector.
4. _presto-accumulo-tools_ - A Java project with some tools to help out with metadata management tasks that could not otherwise be done using SQL.

### Dependencies
* Java 1.8 (required for connector)
* Java 1.7 (`accumulo.jdk.version == 1.7 ? required : !required`)
* Maven
* Accumulo
* _presto_ (Built and installed from this repository)
* _presto-accumulo-iterators_ (Built and installed from this repository)

### Installing the Patched Presto
Change directories to `presto`, build the server with Maven, and deploy this version.  This version contains the `presto-accumulo` module.
```bash
# After cloning the repository:
cd presto/

# Install patched presto into Maven repository (Best go get some coffee -- these'll take a while)
mvn clean install -DskipTests

# Best go get some coffee -- these'll both take
# Package presto
mvn clean package -DskipTests

# Install presto-server package and configure Presto as normal per their documentation
cp presto-server/target/presto-server-0.142-ANY.tar.gz # some place

# You can use this Presto CLI jar file
cp presto-cli/target/presto-cli-0.142-ANY-executable.jar # some place
```

### Installing the Iterator Dependency
There is a separate Maven project in this repository, `presto-accumulo-iterators`, which contains customer iterators in order to push various information in a SQL WHERE clause to Accumulo for server-side filtering; this is know as _predicate pushdown_.  Build this jar file using Maven (which will also install the dependency in your local Maven repository), and push the file out to Accumulo's `lib/ext` directory on every Accumulo TabletServer node.

This Maven build uses JDK v1.7.  Change the value of `project.build.targetJdk` in `pom.xml` to `1.8` in order to build a JDK 1.8-compliant version.

```bash
cd presto-accumulo-iterators/

# Update pom.xml to JDK 1.8 if necessary, i.e. Accumulo JVM is JDK 1.8
mvn clean install -DskipTests

# For each TabletServer node:
scp target/presto-accumulo-iterators-0.*.jar [tabletserver_address]:$ACCUMULO_HOME/lib/ext

# TabletServer should pick up new JAR files in ext directory, but may require restart
```

### Connector Configuration
See `presto-accumulo/etc/catalog/accumulo.properties` for an example configuration of the Accumulo connector.  Fill in the appropriate values to fit your cluster, then copy this file into `$PRESTO_HOME/etc/catalog`.  The list of configuration variables is below.

Restart the Presto server, then use the presto client with the `accumulo` catalog like so:
```bash
presto --server localhost:8080 --catalog accumulo --schema default
```
__*Configuration Variables*__

| Parameter Name                   | Default Value    | Required | Description                                                                                        |
|----------------------------------|------------------|----------|----------------------------------------------------------------------------------------------------|
| connector.name                   | (none)           | Yes      | Name of the connector. Do not change!                                                              |
| instance                         | (none)           | Yes      | Name of the Accumulo instance                                                                      |
| zookeepers                       | (none)           | Yes      | ZooKeeper connect string                                                                           |
| username                         | (none)           | Yes      | Accumulo user for Presto                                                                           |
| password                         | (none)           | Yes      | Accumulo password for user                                                                         |
| zookeeper.metadata.root          | /presto-accumulo | No       | Root znode for storing metadata. Only relevant if using default Metadata Manager                   |
| metadata.manager.class           | default          | No       | Fully qualified classname for the Metadata Manager class.  Default is the ZooKeeperMetadataManager |
| cardinality.cache.size           | 100000           | No       | Gets the size of the index cardinality cache                                                       |
| cardinality.cache.expire.seconds | 300              | No       | Gets the expiration, in seconds, of the cardinality cache                                          |
### Unsupported Features
Of the available Presto DDL/DML statements and features, the Accumulo connector does __not__ support:
* __*Adding columns via ALTER TABLE*__ : Use the `presto-accumulo-tools` subproject for adding new columns
  * See the README in the `presto-accumulo-tools` folder for more details.
* __*Views*__ : CREATE/DROP VIEW is not yet implemented for the connector
* __*Transactions*__ : Transaction support was added in version 0.132 and has not yet been implemented for the connector

### Usage
Simply begin using SQL to create a new table in Accumulo to begin working with data.  By default, the first column of the table definition is set to the Accumulo row ID.  This should be the primary key of your table, and keep in mind that any inserts of a row containing the same row ID is effectively an UPDATE as far as Accumulo is concerned, as the cell of the table will overwrite any existing rows.  The row ID can be any valid Presto datatype.  You can set the row ID using the `row_id` table property within the `WITH` clause of your table definition if you want the row ID to be in a different column than the first.  Simply set this property to the name of the presto column.

When creating a table using SQL, you __must__ specify a `column_mapping` table property.  The value of this property is a comma-delimited list of triples, -- presto column __:__ accumulo column family __:__ accumulo column qualifier, with one triple for every non-row ID column.  This sets the mapping of the Presto column name to the corresponding Accumulo column family and column qualifier.  For example:

```SQL
CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
  column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date');
```
You can then issue INSERT statements to put data into Accumulo.  __*WARNING*__: While issuing `INSERT` statements sure is convenient, this method of loading data into Accumulo is low-throughput.  You'll want to write `Mutations` (and index `Mutations`) directly to the tables if throughput is a concern.  And why wouldn't it be?

```SQL
INSERT INTO myschema.scientists VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
('row2', 'Alan Turing', 103, DATE '1912-06-23' );

SELECT * FROM myschema.scientists;
 recordkey |     name     | age |  birthday  
-----------+--------------+-----+------------
 row1      | Grace Hopper | 109 | 1906-12-09 
 row2      | Alan Turing  | 103 | 1912-06-23 
(2 rows)
```
As you'd expect, rows inserted into Accumulo via the shell or programatically will also show up when queried. (The Accumulo shell thinks "-5321" is an option and not a number... so we'll just make TBL a little younger.)
```
$ accumulo shell -u root -p secret
root@default> table myschema.scientists
root@default myschema.scientists> insert row3 metadata name "Tim Berners-Lee"
root@default myschema.scientists> insert row3 metadata age 60
root@default myschema.scientists> insert row3 metadata date 5321
```
```SQL
presto:default> SELECT * FROM myschema.scientists;
 recordkey |      name       | age |  birthday  
-----------+-----------------+-----+------------
 row1      | Grace Hopper    | 109 | 1906-12-09 
 row2      | Alan Turing     | 103 | 1912-06-23 
 row3      | Tim Berners-Lee |  60 | 1984-07-27 
(3 rows)
```
You can also drop tables using the DROP command.  This command drops both metadata and the tables.  See the below section on [External Tables](#external-tables) for more details on internal and external tables.
```SQL
DROP TABLE myschema.scientists;
```
### External Tables
By default, the tables created using SQL statements via Presto are _internal_ tables, that is both the Presto table metadata and the Accumulo tables are managed by Presto.  When you create an internal table, the Accumulo table is created as well.  You will receive an error if the Accumulo table already exists.  When an internal table is dropped via Presto, the Accumulo table (and any index tables) are dropped as well.

To change this behavior, set the `external` property to `true` when issuing the `CREATE` statement.  This will make the table an _external_ table, and a `DROP TABLE` command will __only__ delete the metadata associated with the table -- the Accumulo tables and data remain untouched.

Note that the Accumulo table and any index tables (if applicable) must exist prior to creating the external table. First, we create an Accumulo table called `external_table`.
```SQL
root@default> createtable external_table
```
Next, we create the Presto external table.
```SQL
presto:default> CREATE TABLE external_table (a VARCHAR, b BIGINT, c DATE) 
WITH (
    column_mapping = 'a:md:a,b:md:b,c:md:c',
    external = true
);
```
After creating the table, usage of the table continues as usual:
```SQL
presto:default> INSERT INTO external_table VALUES ('1', 1, DATE '2015-03-06'), ('2', 2, DATE '2015-03-07');
INSERT: 2 rows

presto:default> SELECT * FROM external_table;
 a | b |     c      
---+---+------------
 1 | 1 | 2015-03-06 
 2 | 2 | 2015-03-06 
(2 rows)

presto:default> DROP TABLE external_table;
DROP TABLE
```
After dropping the table, the table will still exist in Accumulo because it is _external_.
```SQL
root@default> tables
accumulo.metadata
accumulo.root
external_table
trace
```

### Secondary Indexing
Internally, the connector creates an Accumulo `Range` and packs it in a split.  This split gets passed to a Presto Worker to read the data from the `Range` via a `BatchScanner`.  When issuing a query that results in a full table scan, each Presto Worker gets a single `Range` that maps to a single tablet of the table.  When issuing a query with a predicate (i.e. `WHERE x = 10` clause), Presto passes the values within the predicate (`10`) to the connector so it can use this information to scan less data.  When the Accumulo row ID is queried on, this is a single-value `Range` lookup and we can quickly retrieve the data from Accumulo.

But what about the other columns?  If you're frequently querying on non-row ID columns, you should consider using the `secondary indexing` feature built into the Accumulo connector.  This feature can drastically reduce query runtime when selecting a handful of values from the table, and the heavy lifting is done for you when loading data via Presto `INSERT` statements (though, keep in mind writing data to Accumulo via `INSERT` does not have high throughput.  There is an indexing tool in the API to assist in automatically indexing your Accumulo tables.

To enable indexing, add the `index_columns` table property and specify a comma-delimited list of Presto column names you wish to index (we also use the `string` serializer here to help with this example -- you should be using the default `lexicoder`).
```SQL
presto:default> CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
  column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
  serializer = 'string',
  index_columns='name,age,birthday'
);
```
After creating the table, we see there are an additional two Accumulo tables to store the index and metrics.
```
root@default> tables
accumulo.metadata
accumulo.root
myschema.scientists
myschema.scientists_idx
myschema.scientists_idx_metrics
trace
```
After inserting data, we can look at the index table and see there are indexed values for the name, age, and birthday columns.  The connector queries this index table

```SQL
presto:default> INSERT INTO myschema.scientists VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
('row2', 'Alan Turing', 103, DATE '1912-06-23' );

root@default> scan -t myschema.scientists_idx
-21011 metadata_date:row2 []
-23034 metadata_date:row1 []
103 metadata_age:row2 []
109 metadata_age:row1 []
Alan Turing metadata_name:row2 []
Grace Hopper metadata_name:row1 []

```
When issuing a query with a `WHERE` clause against indexed columns, the connector searches the index table for all row IDs that contain the value within the predicate.  These row IDs are bundled into a Presto split as single-value `Range` objects (the number of row IDs per split is controlled by the value of `accumulo.index_rows_per_split`) and passed to Presto to query.

```SQL
presto:default> SELECT * FROM myschema.scientists WHERE age = 109;
 recordkey |     name     | age |  birthday  
-----------+--------------+-----+------------
 row1      | Grace Hopper | 109 | 1906-12-09 
(1 row)
```

### Session Properties
You can change the default value of a session property by using the SET SESSION clause at the top of your Presto script:
```SQL
SET SESSION accumulo.column_filter_optimizations_enabled = false;
```
| Property Name                               | Default Value | Description                                                                                                                                                                                     |
|---------------------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| accumulo.optimize_column_filters_enabled    | false         | Experimental. Set to true to enable the column value filter pushdowns. Default false.  Should stay false, for now.                                                                              |
| accumulo.optimize_locality_enabled          | true          | Set to true to enable data locality for non-indexed scans. Default true.                                                                                                                        |
| accumulo.optimize_split_ranges_enabled      | true          | Set to true to split non-indexed queries by tablet splits. Should generally be true.                                                                                                            |
| accumulo.optimize_index_enabled             | true          | Set to true to enable usage of the secondary index on query. Default true.                                                                                                                      |
| accumulo.index_rows_per_split               | 10000         | The number of Accumulo row IDs that are packed into a single Presto split. Default 10000                                                                                                        |
| accumulo.index_threshold                    | .2            | The ratio between number of rows to be scanned based on the secondary index over the total number of rows. If the ratio is below this threshold, the secondary index will be used. Default 0.2. |
| accumulo.index_lowest_cardinality_threshold | .01           | The threshold where the column with the lowest cardinality will be used instead of computing an intersection of ranges in the secondary index. Secondary index must be enabled.  Default .01.   |

### Serializers
The Presto connector for Accumulo has a pluggable serializer framework for handling I/O between Presto and Accumulo.  This enables end-users the ability to programatically serialized and deserialize their special data formats within Accumulo, while abstracting away the complexity of the connector itself.

There are two types of serializers currently available; a `string` serializer that treats values as Java `String` and a `lexicoder` serializer that leverages Accumulo's Lexicoder API to store values.  The default serializer is the `lexicoder` serializer, as this serializer does not require expensive conversion operations back and forth between `String` objects and the Presto types -- the object is encoded as a byte array.  

You can change the default the serializer by specifying the `serializer` table property, using either `default` (which is `lexicoder`), `string` or `lexicoder` for the built-in types, or you could provide your own implementation by extending `AccumuloRowSerializer`, adding it to the Presto `CLASSPATH`, and specifying the fully-qualified Java class name.

```SQL
presto:default> CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
    serializer = 'default'
);

presto:default> INSERT INTO myschema.scientists VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
('row2', 'Alan Turing', 103, DATE '1912-06-23' );

root@default> scan -t myschema.scientists
row1 metadata:age []    \x08\x80\x00\x00\x00\x00\x00\x00m
row1 metadata:date []    \x08\x7F\xFF\xFF\xFF\xFF\xFF\xA6\x06
row1 metadata:name []    Grace Hopper
row2 metadata:age []    \x08\x80\x00\x00\x00\x00\x00\x00g
row2 metadata:date []    \x08\x7F\xFF\xFF\xFF\xFF\xFF\xAD\xED
row2 metadata:name []    Alan Turing

presto:default> CREATE TABLE myschema.stringy_scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
    serializer = 'string'
);

presto:default> INSERT INTO myschema.stringy_scientists VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
('row2', 'Alan Turing', 103, DATE '1912-06-23' );

root@default> scan -t myschema.stringy_scientists
row1 metadata:age []    109
row1 metadata:date []    -23034
row1 metadata:name []    Grace Hopper
row2 metadata:age []    103
row2 metadata:date []    -21011
row2 metadata:name []    Alan Turing

CREATE TABLE myschema.custom_scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
    serializer = 'my.serializer.package.MySerializer'
);
```

### Metadata Management
Metadata management for the Accumulo tables is pluggable, with an initial implementation storing the data in ZooKeeper.  You can (and should) issue SQL statements in Presto to create and drop tables.  This is the easiest method of creating the metadata required to make the connector work.  It is best to not mess with the metadata, but here are the details of how it is stored.  Information is power.

A root node in ZooKeeper holds all the mappings, and the format is as follows:
```bash
/metadata-root/schema/table
```
Where `metadata-root` is the value of `zookeeper.metadata.root` in the config file (default is `/presto-accumulo`), `schema` is the Presto schema (which is identical to the Accumulo namespace name), and `table` is the Presto table name (again, identical to Accumulo name).  The data of the `table` ZooKeeper node is a serialized `AccumuloTable` Java object (which resides in the connector code).  This table contains the schema (namespace) name, table name, column definitions, the serializer to use for the table, and any additional metadata. 

If you have a need to programmatically manipulate the ZooKeeper metadata for Accumulo, take a look at `bloomberg.presto.accumulo.metadata.ZooKeeperMetadataManager` for some Java code to simplify the process.
