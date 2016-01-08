# presto-accumulo

A Presto connector for working with data stored in Apache Accumulo

### Dependencies
* Java 1.8 (required for connector)
* Java 1.7 (`accumulo.jdk.version == 1.7 ? required : !required`)
* Maven
* Presto
* Accumulo
* presto-accumulo-iterators

### Installing the Iterator Dependency
There is a separate Maven project in this repository, `presto-accumulo-iterators`, which contains customer iterators in order to push various information in a SQL WHERE clause to Accumulo for server-side filtering.  Build this jar file using Maven (which will also install the dependency in your local Maven repository), and push the file out to Accumulo's `lib` directory on every Accumulo TabletServer node.

This Maven build uses JDK v1.7.  Change the value of `project.build.targetJdk` in `pom.xml` to `1.8` in order to build a JDK 1.8-compliant version.

```bash
# After cloning the repository:
cd presto-accumulo-iterators/

# Update pom.xml to JDK 1.8 if necessary
mvn clean install

# For each TabletServer node:
scp target/presto-accumulo-iterators-0.*.jar <tabletserver_address>:$ACCUMULO_HOME/lib
```

### Installing the Connector
In order to use the connector, build the presto-accumulo connector using Maven.  This will create a directory containing all JAR file dependencies for the connector to function.  Copy the contents of this directory into an ```accumulo``` directory under ```$PRESTO_HOME/plugin```.

```bash 
# After cloning the repository:
cd presto-accumulo/
mvn clean package
mkdir -p $PRESTO_HOME/plugin/accumulo/
cp target/presto-accumulo-0.*/* $PRESTO_HOME/plugin/accumulo/
```

### Configuration
See ```etc/catalog/accumulo.properties``` for an example configuration of the Accumulo connector.  Fill in the appropriate values to fit your cluster, then copy this file into ```$PRESTO_HOME/etc/catalog```.

Restart the Presto server, then use the presto client with the ```accumulo``` catalog like so:
```bash
presto --server localhost:8080 --catalog accumulo --schema default
```

### Usage
Simply begin using SQL to create a new table in Accumulo to begin working with data.  By default, the first column of the table definition is set to the Accumulo row ID.  This should be the primary key of your table.  It can be any valid Presto datatype.  You can set the row ID using the `row_id` table property within the `WITH` clause of your table definition if you want the row ID to be in a different column than the first.  Simply set this property to the name of the presto column.

When creating a table using SQL, you __must__ specify a `column_mapping` table property.  The value of this property is a comma-delimited list of triples, -- presto column __:__ accumulo column family __:__ accumulo column qualifier, with one triple for every non-row ID column.  This sets the mapping of the presto column name to the corresponding Accumulo column family and column qualifier.  See below for an example

```SQL
CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date');
```
If the Accumulo table already exists, you can also set the ```metadata_only``` table property (which defaults to ```false```) in order to skip creating the Accumulo table and just create the metadata for the table.  This is the easiest way of creating the metadata for existing Accumulo tables.
```SQL
CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    metadata_only = true, 
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date'
);
USE myschema;
SHOW TABLES;
   Table    
------------
 scientists 
(1 row)
```
You can then issue INSERT statements to put data into Accumulo.  Note that this functionality is experimental.
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
root@default> table myschema.scientists
root@default myschema.scientists> insert row3 metadata name "Tim Berners-Lee"
root@default myschema.scientists> insert row3 metadata age 60
root@default myschema.scientists> insert row3 metadata date 5321
```
```SQL
SELECT * FROM myschema.scientists;
 recordkey |      name       | age |  birthday  
-----------+-----------------+-----+------------
 row1      | Grace Hopper    | 109 | 1906-12-09 
 row2      | Alan Turing     | 103 | 1912-06-23 
 row3      | Tim Berners-Lee |  60 | 1984-07-27 
(3 rows)
```
You can also drop tables using the DROP command.
```SQL
DROP TABLE myschema.scientists;
```
As you can see via the Accumulo shell snippet below, this command drops __metadata only__, unless you've specified the `internal` table property to `true`. See the below section on [Internal Tables](#internal-tables)  for more information.
 ```
root@default> tables
accumulo.metadata
accumulo.root
myschema.scientists
trace
```
### Internal Tables

By default, the tables created using SQL statements via Presto are _external_ tables, that is the table backing the data in Accumulo will not be deleted when the table is dropped.  To change this behavior, set the `internal` property to `true` when issuing the `CREATE` statement.  This will make the table an _internal_ table, and a `DROP TABLE` command will also delete the corresponding Accumulo table.  Ye be warned.
```SQL
CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
    internal = true
);
```
 In the Accumulo shell:
```
root@default> tables
accumulo.metadata
accumulo.root
myschema.scientists
trace
```
When the table is dropped:
```SQL
DROP TABLE myschema.scientists;
```
 All gone!
```
root@default> tables
accumulo.metadata
accumulo.root
trace
```
### Serializers
The Presto connector for Accumulo has a pluggable serializer framework for handling I/O between Presto and Accumulo.  This enables end-users the ability to programatically serialized and deserialize their special data formats within Accumulo, while abstracting away the complexity of the connector itself.

There are two types of serializers currently available; a ```String``` serializer and a serializer that uses Accumulo's various lexicoders.  The default serializer expects the values in your Accumulo table to be stored as a Java ```String```, that is instead of storing four bytes of an integer, the number is stored as a String.

You can change the serializer by specifying the ```serializer``` table property, using either ```string``` or ```lexicoder``` for the built-in types, or you could provide the fully qualified Java class name.

```SQL
CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
    serializer = 'string'
);

CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
    serializer = 'lexicoder'
);

CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
    serializer = 'my.serializer.package.MySerializer'
);
```

### Metadata Management
Metadata management for the Accumulo tables is pluggable, with an initial implementation storing the data in ZooKeeper.  You can (and should) issue SQL statements in Presto to create and drop tables.  This is the easiest method of creating the metadata required to make the connector work.  It is best to not mess with the metadata, but here are the details of how it is stored.  Information is power.

A root node in ZooKeeper holds all the mappings, and the format is as follows:
```bash
/<metadata-root>/<schema>/<table>
```
Where `<metadata-root` is the value of `zookeeper.metadata.root` in the config file (default is `/presto-accumulo`), `<schema>` is the Presto schema (which is identical to the Accumulo namespace name), and `<table>` is the Presto table name (again, identical to Accumulo name).  The data of the ```<table>``` ZooKeeper node is a serialized ```AccumuloTable``` Java object (which resides in the connector code).  This table contains the schema (namespace) name, table name, column definitions, and the serializer to use for the table. 

If you have a need to programmatically manipulate the ZooKeeper metadata for Accumulo, take a look at ```bloomberg.presto.accumulo.metadata.ZooKeeperMetadataManager``` for some Java code to simplify the process.

### Test Data Generation
Some test cases require generation of large files for testing.  This repository includes a file, datagen.py, that can be used to generate these large files.

Dependent on faker which can be installed like so: `pip install fake-factory`

```bash
# No arguments will print the usage and row header
$ python datagen.py
usage: python datagen.py <numrows> <outfile>
uuid VARCHAR|first_name VARCHAR|last_name VARCHAR|address VARCHAR|city VARCHAR|state VARCHAR|zipcode BIGINT|birthday DATE|favorite_color VARCHAR

# Pass it the number of rows to generate, which can be piped to src/test/resources/file_a.txt for that one test
$ python datagen.py 4 src/test/resources/file_a.txt
$ cat src/test/resources/file_a.txt
9db99016-9334-4d02-9f99-a362f904716d|Burleigh|Schamberger|88868 Marti Mountains Suite 257|Port Damarcusview|Florida|54459|517193378|fuchsia
e5898627-0222-4e76-b33a-76d0c8a3076f|Audrina|Langosh|7008 Phil Lodge|Port Lydell|Wyoming|22723|344114593|silver
4fc6a5bb-27f5-4ea9-98aa-26a885f174c2|Alonso|Smith|448 Kautzer Prairie Apt. 876|West Corettabury|Arkansas|02464|248053641|gray
02832cb9-1a9e-43d6-81d2-3d155df2d88c|Clara|Bailey|30272 Justin Forest|Shareemouth|Wyoming|90367|522550149|white
```
