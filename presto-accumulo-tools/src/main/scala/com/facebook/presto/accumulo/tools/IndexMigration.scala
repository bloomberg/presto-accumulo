/**
 * Copyright 2016 Bloomberg L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.tools

import com.esotericsoftware.kryo.Kryo
import com.facebook.presto.`type`.TypeRegistry
import com.facebook.presto.accumulo.conf.AccumuloConfig
import com.facebook.presto.accumulo.io.PrestoBatchWriter
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager
import com.facebook.presto.spi.SchemaTableName
import com.google.common.base.Preconditions.checkState
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.impl.{ConfiguratorBase, InputConfigurator}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{ClientConfiguration, ZooKeeperInstance}
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

case class IndexMigrationRegistrator() extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Text])
    kryo.register(classOf[Key])
    kryo.register(classOf[Value])
  }
}

class IndexMigration extends Task with Serializable {
  val TASK_NAME = "index-migration"

  private val SRC_TABLE_OPT: Char = 's'
  private val DEST_TABLE_OPT: Char = 'd'
  private val AUTHS_OPT: Char = 'a'
  private val NUM_PARTITIONS_OPT: Char = 'n'

  def getOptions: Options = {
    val opts = new Options

    OptionBuilder.withLongOpt("src-table")
    OptionBuilder.withDescription("Source table to copy from")
    OptionBuilder.isRequired
    OptionBuilder.hasArg
    opts.addOption(OptionBuilder.create(SRC_TABLE_OPT))

    OptionBuilder.withLongOpt("dest-table")
    OptionBuilder.withDescription("Dest table to copy to")
    OptionBuilder.isRequired
    OptionBuilder.hasArg
    opts.addOption(OptionBuilder.create(DEST_TABLE_OPT))

    OptionBuilder.withLongOpt("auths")
    OptionBuilder.withDescription("Authorization string. Note that you'll need to specify all auths to cover all entries in the table to be properly copied")
    OptionBuilder.hasArg
    opts.addOption(OptionBuilder.create(AUTHS_OPT))

    OptionBuilder.withLongOpt("num-partitions")
    OptionBuilder.withDescription("Number of partitions to create when writing to Accumulo")
    OptionBuilder.hasArg
    opts.addOption(OptionBuilder.create(NUM_PARTITIONS_OPT))

    opts
  }

  /**
    * Gets the name of the task/tool to be displayed to the user and used to identify via the
    * command line
    *
    * @return Task name
    */
  override def getTaskName: String = TASK_NAME

  /**
    * Gets the description of the task/tool to be displayed to the user
    *
    * @return Task description
    */
  override def getDescription = "Copies the data of a Presto/Accumulo to a new table using the new indexing methods"


  override def run(conf: AccumuloConfig, cmd: CommandLine): Int = {
    val instance = conf.getInstance
    val zooKeepers = conf.getZooKeepers
    val username = conf.getUsername
    val password = conf.getPassword
    val srcTableName = cmd.getOptionValue(SRC_TABLE_OPT)
    val destTableName = cmd.getOptionValue(DEST_TABLE_OPT)

    val numPartitions = if (cmd.hasOption(NUM_PARTITIONS_OPT)) {
      cmd.getOptionValue(NUM_PARTITIONS_OPT).toInt
    } else {
      20
    }

    val auths = if (cmd.hasOption(AUTHS_OPT)) {
      new Authorizations(cmd.getOptionValue(AUTHS_OPT).split(","): _*)
    } else {
      new Authorizations
    }

    val connector = new ZooKeeperInstance(instance, zooKeepers).getConnector(username, new PasswordToken(password))

    checkState(connector.tableOperations().exists(srcTableName), "source table %s does not exist", srcTableName)
    checkState(connector.tableOperations().exists(destTableName), "destination table %s does not exist, create the Presto table and Accumulo tables", destTableName)

    val spark = SparkSession.builder
      .appName("IndexMigration")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "com.facebook.presto.accumulo.tools.IndexMigrationRegistrator")
      .getOrCreate()

    val jobConf = new Configuration()

    val clientConfig = new ClientConfiguration()
    clientConfig.withInstance(instance)
    clientConfig.withZkHosts(zooKeepers)

    ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], jobConf, clientConfig)
    ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], jobConf, username, new PasswordToken(password))
    InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], jobConf, auths)
    InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], jobConf, srcTableName)

    val rdd = spark.sparkContext.newAPIHadoopRDD(jobConf, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])
      .repartition(numPartitions)
      .persist(MEMORY_AND_DISK)

    // Group entries by key and then write each partition to Accumulo via the PrestoBatchWriter
    rdd.groupBy(entry => entry._1.getRow).foreachPartition(partition => writePartition(instance, zooKeepers, username, password, destTableName, auths, partition))

    spark.stop()
    0
  }

  def writePartition(instance: String, zooKeepers: String, username: String, password: String, destTableName: String, auths: Authorizations, partition: Iterator[(Text, Iterable[(Key, Value)])]): Unit = {
    val schemaTableName =
      if (destTableName.contains(".")) {
        new SchemaTableName(destTableName.split("\\.")(0), destTableName.split("\\.")(1))
      } else {
        new SchemaTableName("default", destTableName)
      }

    val accumuloConfig = new AccumuloConfig()
    accumuloConfig.setInstance(instance)
    accumuloConfig.setZooKeepers(zooKeepers)
    accumuloConfig.setUsername(username)
    accumuloConfig.setPassword(password)

    val connector = new ZooKeeperInstance(instance, zooKeepers).getConnector(username, new PasswordToken(password))
    val table = new ZooKeeperMetadataManager(accumuloConfig, new TypeRegistry()).getTable(schemaTableName)

    val batchWriter = new PrestoBatchWriter(connector, auths, table)
    var numRows = 0
    partition.foreach(row => {
      val cf = new Text
      val cq = new Text
      val m = new Mutation(row._1)
      row._2.foreach(entry => {
        entry._1.getColumnFamily(cf)
        entry._1.getColumnQualifier(cq)
        m.put(cf, cq, entry._1.getColumnVisibilityParsed, entry._1.getTimestamp, entry._2)
      })
      batchWriter.addMutation(m)

      numRows += 1
      if (numRows % 100000 == 0) {
        batchWriter.flush()
        numRows = 0
      }
    })

    batchWriter.close()
  }
}