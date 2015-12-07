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
-z localhost:2181 -n default -t mytable -f cfa -q cb -c col_a -p INTEGER
```

