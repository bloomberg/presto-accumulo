# presto-accumulo

Presto Accumulo Integration

### Metadata Management

The column metadata for the Accumulo tables is pluggable, and an initial implementation is provided for metadata stored in ZooKeeper.

A root node in ZooKeeper holds all the mappings, and the format is as follows:
```bash
/<metadata-root>/<schema>/<table>/<presto-col>
```
Where `<metadata-root` is the value of `zookeeper.metadata.root` in the config file (default is `/presto-accumulo`), `<schema>` is the Presto schema (which is identical to the Accumulo schema name), `<table>` is the Presto table name (again, identical to Accumulo name), and `presto-col` is the name of the presto column.  The data of this ZK node is a JSON string of the following format.  This contains the metadata mappings for transforming values stored in an Accumulo table to a presto table.

```json
{"name":"cola","family":"cf1","qualifier":"cq1","type":"varchar"}
```

The mappings must be created by hand (for now).  There is a Java class and a command-line wrapper for assisting in creating this mapping in ZooKeeper.

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

### Test Data Generation

Some test cases require generation of large files for testing.  This repository includes a file, datagen.py, that can be used to generate these large files.

```bash
# No arguments will print the usage and row header
$ python datagen.py 
usage: python datagen.py <numrows>
uuid VARCHAR|first_name VARCHAR|last_name VARCHAR|address VARCHAR|city VARCHAR|state VARCHAR|zipcode BIGINT|birthday DATE|favorite_color VARCHAR

# Pass it the number of rows to generate, which can be piped to src/test/resources/file_a.txt for that one test
$ python datagen.py 4 > src/test/resources/file_a.txt
$ cat src/test/resources/file_a.txt
9db99016-9334-4d02-9f99-a362f904716d|Burleigh|Schamberger|88868 Marti Mountains Suite 257|Port Damarcusview|Florida|54459|517193378|fuchsia
e5898627-0222-4e76-b33a-76d0c8a3076f|Audrina|Langosh|7008 Phil Lodge|Port Lydell|Wyoming|22723|344114593|silver
4fc6a5bb-27f5-4ea9-98aa-26a885f174c2|Alonso|Smith|448 Kautzer Prairie Apt. 876|West Corettabury|Arkansas|02464|248053641|gray
02832cb9-1a9e-43d6-81d2-3d155df2d88c|Clara|Bailey|30272 Justin Forest|Shareemouth|Wyoming|90367|522550149|white
```

