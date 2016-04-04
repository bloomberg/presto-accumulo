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

### Dependencies
* Java 1.7
* Maven
* Accumulo
* _presto-accumulo_ (Built and installed from this repository)

### Usage
Build the `presto-accumulo-tools` jar file using Maven, then execute the jar file to see all available tools:

```bash
$ cd presto-accumulo-tools/
$ mvn clean package
$ java -jar target/presto-accumulo-tools-0.131.jar 
Usage: java -jar <jarfile> <tool> [args]
Available tools:
	addcolumn <schema.name> <table.name> <presto.name> <presto.type> <column.family> <column.qualifier> <indexed> [zero.based.ordinal]

```

### Available Tools
#### addcolumn
Adds a new column to an existing table.  This cannot be done today via `ALTER TABLE [table] ADD COLUMN [name] [type]` because of the additional metadata required for the columns to work; the column family, qualifier, and if the column is indexed.

The table must exist prior to using this tool, and the Presto column name must not exist (no duplicate column names).  The last argument is an optional zero-based ordinal of where to put the column.  Default is to put the new column at the end of the row, but you can insert the column at whatever ordinal you please.

__*Example Usage*__

```SQL
presto:default> CREATE TABLE foo (a BIGINT, b BIGINT) WITH (column_mapping = 'b:b:b');
CREATE TABLE

presto:default> INSERT INTO foo VALUES (1,1), (2,2);
INSERT: 2 rows

presto:default> DESCRIBE foo;
 Column |  Type  | Null | Partition Key |               Comment               
--------+--------+------+---------------+-------------------------------------
 a      | bigint | true | false         | Accumulo row ID                     
 b      | bigint | true | false         | Accumulo column b:b. Indexed: false 
(2 rows)

presto:default> SELECT * FROM foo;
 a | b 
---+---
 1 | 1 
 2 | 2 
(2 rows)
```
And now we invoke the program to insert a column `c`, type `VARCHAR`, mapped to the Accumulo column family `c` and qualifier `c`.  The column is indexed and inserted at ordinal one:
```bash
$ java -jar target/presto-accumulo-tools-0.131.jar addcolumn default foo c varchar c c true 1
# ... bunch of log statements

```
Now we describe the table and query it again:
```SQL
presto:default> DESCRIBE foo;
 Column |  Type   | Null | Partition Key |               Comment               
--------+---------+------+---------------+-------------------------------------
 a      | bigint  | true | false         | Accumulo row ID                     
 c      | varchar | true | false         | Accumulo column c:c. Indexed: true  
 b      | bigint  | true | false         | Accumulo column b:b. Indexed: false 
(3 rows)

presto:default> SELECT * FROM foo;
 a |  c   | b 
---+------+---
 1 | NULL | 1 
 2 | NULL | 2 
(2 rows)
```