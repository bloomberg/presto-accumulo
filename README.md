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
CREATE TABLE myschema.mytable (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date');
```
If the Accumulo table already exists, you can also set the ```metadata_only``` table property (which defaults to ```false```) in order to skip creating the Accumulo table and just create the metadata for the table.  This is the easiest way of creating the metadata for existing Accumulo tables.
```SQL
CREATE TABLE myschema.myexistingtable (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
WITH (
    metadata_only = true, 
    column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date'
);
USE myschema;
SHOW TABLES;
      Table      
-----------------
 myexistingtable 
 mytable         
(2 rows)
```
You can also drop tables using the DROP command.  This command drops __metadata only__.  Dropping your Accumulo table seems wildly unsafe.  You'll have to do that in the Accumulo shell.
```SQL
DROP TABLE myschema.mytable;
DROP TABLE myschema.myexistingtable;
```
### Metadata Management

The column metadata for the Accumulo tables is pluggable, and an initial implementation is provided for metadata stored in ZooKeeper.  You can (and should) issue SQL statements in Presto to create and drop tables.  This is the easiest method of creating the metadata required to make the connector work.  The details of how metadata is managed in ZooKeeper is not necessary to use the connector, but here are the details anyway.

A root node in ZooKeeper holds all the mappings, and the format is as follows:
```bash
/<metadata-root>/<schema>/<table>/<presto-col>
```
Where `<metadata-root` is the value of `zookeeper.metadata.root` in the config file (default is `/presto-accumulo`), `<schema>` is the Presto schema (which is identical to the Accumulo schema name), `<table>` is the Presto table name (again, identical to Accumulo name), and `presto-col` is the name of the presto column.  The data of this ZK node is a JSON string of the following format.  This contains the metadata mappings for transforming values stored in an Accumulo table to a presto table.

```json
{"name":"cola","family":"cf1","qualifier":"cq1","type":"varchar"}
```

There is a Java class and a command-line wrapper for assisting in creating this mapping in ZooKeeper.

```bash
# Copy presto dependencies to the accumulo lib directory
cp $PRESTO_HOME/lib/presto-spi-0.128.jar $ACCUMULO_HOME/lib/
cp $PRESTO_HOME/lib/slice-0.15.jar $ACCUMULO_HOME/lib/
cp $PRESTO_HOME/lib/jackson-databind-2.4.4.jar $ACCUMULO_HOME.lib/
cp $PRESTO_HOME/lib/jackson-core-2.4.4.jar $ACCUMULO_HOME/lib/
cp $PRESTO_HOME/lib/jackson-annotations-2.4.4.jar $ACCUMULO_HOME/lib/

# An example column name, use --help for details
accumulo jar ~/gitrepos/presto-accumulo/target/presto-accumulo-0.128.jar \
bloomberg.presto.accumulo.metadata.ZooKeeperMetadataCreator \
-z localhost:2181 -n default -t mytable -f cfa -q cb -c col_a -p BIGINT
```
If you have a need to programmatically manipulate the ZooKeeper metadata for Accumulo, take a look at ```bloomberg.presto.accumulo.metadata.ZooKeeperMetadataCreator``` for some Java code to simplify the process.

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

