# presto-accumulo

A Presto connector for working with data stored in Apache Accumulo

### Installation

In order to use the connector, build the presto-accumulo connector using Maven.  This will create a directory containing all JAR file dependencies for the connector to function.  Copy the contents of this directory into an ```accumulo``` directory under ```$PRESTO_HOME/plugin```.

```bash 
git clone <url>
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
Simply begin using SQL to create a new table in Accumulo to begin working with data.  The first column of the table definition is expected to be ```recordkey VARCHAR```, which maps to the Accumulo row ID.  After that, specify the presto column names and data types.

When creating a table using SQL, you __must__ specify a ```column_mapping``` table property.  The value of this property is a comma-delimited list of triples -- presto column __:__ accumulo column family __:__ accumulo column qualifier.  This sets the mapping of the presto column name to the corresponding Accumulo column family and column qualifier.  See below for an example

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
You can also drop tables using the DROP command.  This command drops __metadata only__.  Dropping your Accumulo table seems wildly unsafe.  You'll have to do that in the Accumulo shell.
```SQL
DROP TABLE myschema.scientists;
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
