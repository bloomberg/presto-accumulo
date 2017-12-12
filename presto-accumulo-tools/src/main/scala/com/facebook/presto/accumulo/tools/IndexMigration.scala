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

import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap
import java.text.SimpleDateFormat
import java.util.Date
import java.util.stream.Collectors

import com.esotericsoftware.kryo.Kryo
import com.facebook.presto.`type`.TypeRegistry
import com.facebook.presto.accumulo.conf.AccumuloConfig
import com.facebook.presto.accumulo.index.Indexer
import com.facebook.presto.accumulo.index.Indexer.Destination
import com.facebook.presto.accumulo.index.Indexer.Destination.{INDEX, METRIC}
import com.facebook.presto.accumulo.index.metrics.AccumuloMetricsStorage.{CARDINALITY_CF, CARDINALITY_CQ}
import com.facebook.presto.accumulo.index.metrics.MetricsStorage.{METRICS_TABLE_ROWS_COLUMN, METRICS_TABLE_ROW_ID}
import com.facebook.presto.accumulo.index.storage.ShardedIndexStorage
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager
import com.facebook.presto.accumulo.tools.MultiOutputRDD._
import com.facebook.presto.spi.SchemaTableName
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions.checkState
import com.google.common.collect.{ImmutableList, ListMultimap, MultimapBuilder}
import com.google.common.primitives.Bytes
import org.apache.accumulo.core.client.mapreduce.lib.impl.{ConfiguratorBase, InputConfigurator}
import org.apache.accumulo.core.client.mapreduce.{AccumuloFileOutputFormat, AccumuloInputFormat}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import org.apache.accumulo.core.data.{ColumnUpdate, Key, Mutation, Value, Range => AccumuloRange}
import org.apache.accumulo.core.iterators.LongCombiner
import org.apache.accumulo.core.iterators.LongCombiner.FixedLenEncoder
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.cli.{CommandLine, OptionBuilder, Options}
import org.apache.commons.lang3.tuple.Pair
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{DataInputBuffer, Text}
import org.apache.hadoop.mapred.RawKeyValueIterator
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.counters.GenericCounter
import org.apache.hadoop.mapreduce.lib.output.{LazyOutputFormat, MultipleOutputs}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl.DummyReporter
import org.apache.hadoop.mapreduce.task.{ReduceContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.util.Progress
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SerializableWritable, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

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
  private val WORK_DIR_OPT: Char = 'w'
  private val OFFLINE_OPT: Char = 'o'
  private val NUM_SPLITS_PER_JOB_OPT: Char = 'j'
  private val EMPTY_BYTES: Array[Byte] = new Array[Byte](0)

  private var spark: Option[SparkSession] = None

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

    OptionBuilder.withLongOpt("work-dir")
    OptionBuilder.withDescription("Working directory for the bulk import")
    OptionBuilder.hasArg
    opts.addOption(OptionBuilder.create(WORK_DIR_OPT))

    OptionBuilder.withLongOpt("offline")
    OptionBuilder.withDescription("Enable run an offline table scan (requires table to be offline)")
    opts.addOption(OptionBuilder.create(OFFLINE_OPT))

    OptionBuilder.withLongOpt("num-splits-per-job")
    OptionBuilder.withDescription("Sets the number of splits for each spark job, splitting the ingestion across multiple ranges")
    OptionBuilder.hasArg
    opts.addOption(OptionBuilder.create(NUM_SPLITS_PER_JOB_OPT))

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
    val workDir = cmd.getOptionValue(WORK_DIR_OPT)

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

    val numSplitsPerJob = if (cmd.hasOption(NUM_SPLITS_PER_JOB_OPT)) {
      cmd.getOptionValue(NUM_SPLITS_PER_JOB_OPT).toInt
    } else {
      -1
    }

    val isOfflineScan = cmd.hasOption(OFFLINE_OPT)
    exec(conf, instance, zooKeepers, username, password, srcTableName, destTableName, auths, numPartitions, isOfflineScan, workDir, numSplitsPerJob)
  }

  @VisibleForTesting
  def setSparkSession(session: SparkSession): Unit = {
    spark = Some(session)
  }

  def getSparkSession: SparkSession = {
    spark.getOrElse({
      SparkSession.builder
        .appName("IndexMigration")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "com.facebook.presto.accumulo.tools.IndexMigrationRegistrator")
        .getOrCreate()
    })
  }

  def exec(
            conf: AccumuloConfig,
            instance: String,
            zooKeepers: String,
            username: String,
            password: String,
            srcTableName: String,
            destTableName: String,
            auths: Authorizations,
            numPartitions: Int,
            isOfflineScan: Boolean,
            workDir: String,
            numSplitsPerJob: Int): Int = {
    val connector = new ZooKeeperInstance(instance, zooKeepers).getConnector(username, new PasswordToken(password))

    checkState(connector.tableOperations().exists(srcTableName), "source table %s does not exist", srcTableName)
    checkState(connector.tableOperations().exists(destTableName), "destination table %s does not exist, create the Presto table and Accumulo tables", destTableName)


    val compactionRanges = scala.collection.mutable.ListBuffer[AccumuloRange]()

    val tableSplits = connector.tableOperations.listSplits(srcTableName).stream.sorted.collect(Collectors.toList())
    val numTabletServers = connector.instanceOperations.getTabletServers.size

    if (numSplitsPerJob <= 0) {
      compactionRanges.append(new AccumuloRange())
    } else {
      val compactionPoints: List[Text] = (1 to tableSplits.size()).filter((n: Int) => n % numSplitsPerJob == 0).map((n: Int) => tableSplits.get(n)).toList

      if (compactionPoints.isEmpty) {
        compactionRanges.append(new AccumuloRange())
      } else {
        // Add first range of (-inf, point[0])
        compactionRanges.append(new AccumuloRange(null, true, compactionPoints.head, false))

        // Add all ranges [point[i], point[i+1])
        for (i <- 0 until compactionPoints.size - 1) {
          compactionRanges.append(new AccumuloRange(compactionPoints(i), true, compactionPoints(i + 1), false))
        }

        // Add final range of [point[size-1], +inf)
        compactionRanges.append(new AccumuloRange(compactionPoints.last, null))
      }
    }

    compactionRanges.foreach((range: AccumuloRange) => {
      System.out.println(String.format("range: [%s, %s)", if (range.getStartKey != null) encodeBytes(range.getStartKey.getRow.copyBytes()) else null, if (range.getEndKey != null) encodeBytes(range.getEndKey.getRow().copyBytes) else null))
    })

    System.out.println("%s tablet servers, %s splits in table, %s splits per batch, %s batches".format(numTabletServers, tableSplits.size, numSplitsPerJob, compactionRanges.size))

    val spark = getSparkSession

    for (range <- compactionRanges) {
      runSparkJob(spark, range, connector, instance, zooKeepers, username, password, srcTableName, destTableName, auths, numPartitions, isOfflineScan, workDir)
    }

    spark.stop()
    0
  }

  def runSparkJob(
                   spark: SparkSession,
                   range: AccumuloRange,
                   connector: Connector,
                   instance: String,
                   zooKeepers: String,
                   username: String,
                   password: String,
                   srcTableName: String,
                   destTableName: String,
                   auths: Authorizations,
                   numPartitions: Int,
                   isOfflineScan: Boolean,
                   workDir: String): Unit = {
    val jobConf = new Configuration()

    val clientConfig = new ClientConfiguration()
    clientConfig.withInstance(instance)
    clientConfig.withZkHosts(zooKeepers)

    ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], jobConf, clientConfig)
    ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], jobConf, username, new PasswordToken(password))
    InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], jobConf, auths)
    InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], jobConf, srcTableName)
    InputConfigurator.setOfflineTableScan(classOf[AccumuloInputFormat], jobConf, isOfflineScan)
    InputConfigurator.setRanges(classOf[AccumuloInputFormat], jobConf, ImmutableList.of(range))
    InputConfigurator.setAutoAdjustRanges(classOf[AccumuloInputFormat], jobConf, true)

    // Group entries by key and then write each partition to Accumulo via the PrestoBatchWriter
    spark.sparkContext.newAPIHadoopRDD(jobConf, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])
      .repartition(numPartitions)
      .groupBy(entry => entry._1.getRow)
      .mapPartitions(partition => mapPartition(instance, zooKeepers, username, password, destTableName, partition))
      .sortBy(key => key._2._1)
      .saveAsMultiTextFiles(workDir)

    val outputPath = new Path(workDir)
    val failurePath = new Path(workDir, "failure-%s".format(System.currentTimeMillis().toString))
    val fs = FileSystem.get(jobConf)

    fs.mkdirs(failurePath)

    // Import all non-failure directory tables
    for (dir <- fs.listStatus(outputPath).filter(_.isDirectory).filter(fs => !fs.getPath.getName.equals(failurePath.getName))) {
      val name = dir.getPath.getName
      val tableDir = "%s/%s".format(workDir, name)
      connector.tableOperations().importDirectory(name, tableDir, failurePath.toString, false)
      System.out.println("Imported %s into table %s".format(tableDir, name))
    }

    fs.delete(outputPath, true)
  }

  def mapPartition(instance: String, zooKeepers: String, username: String, password: String, destTableName: String, partition: Iterator[(Text, Iterable[(Key, Value)])]): Iterator[(String, (Key, Value))] = {
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

    val serializer = table.getSerializerInstance
    val encoder = new LongCombiner.FixedLenEncoder

    import scala.collection.JavaConversions._
    val keyValues = new ListBuffer[(String, (Key, Value))]()

    var numRows = 0L
    val cf = new Text
    val cq = new Text
    partition.foreach(row => {
      numRows = numRows + 1

      val mutation = new Mutation(row._1)
      row._2.foreach(entry => {
        entry._1.getColumnFamily(cf)
        entry._1.getColumnQualifier(cq)
        mutation.put(cf, cq, entry._1.getColumnVisibilityParsed, entry._1.getTimestamp, entry._2)

        keyValues.add(("%s/%s".format(destTableName, "data"), (new Key(row._1.getBytes, cf.copyBytes(), cq.copyBytes(), entry._1.getColumnVisibility.copyBytes(), entry._1.getTimestamp, false, true), entry._2)))
      })

      // Convert the list of updates into a data structure we can use for indexing
      val updates = MultimapBuilder.hashKeys().arrayListValues().build().asInstanceOf[ListMultimap[Pair[ByteBuffer, ByteBuffer], ColumnUpdate]]
      for (columnUpdate <- mutation.getUpdates) {
        val family = wrap(columnUpdate.getColumnFamily)
        val qualifier = wrap(columnUpdate.getColumnQualifier)
        updates.put(Pair.of(family, qualifier), columnUpdate)
      }

      for (indexColumn <- table.getParsedIndexColumns.asScala.toList) {
        val indexColumnUpdates: ListMultimap[Destination, ColumnUpdate] = Indexer.getIndexColumnUpdates(table, indexColumn, updates, serializer)

        for (indexValue <- indexColumnUpdates.get(INDEX)) {
          var rowBytes = indexValue.getColumnQualifier
          for (storage <- indexColumn.getIndexStorageMethods.asScala.filter(x => x.isInstanceOf[ShardedIndexStorage])) {
            rowBytes = storage.encode(rowBytes)
          }

          val family = indexValue.getColumnFamily
          val qualifier = row._1.getBytes
          val timestamp = if (indexValue.hasTimestamp) Some(indexValue.getTimestamp) else None
          val visibility = indexValue.getColumnVisibility

          keyValues.add(("%s/%s".format(indexColumn.getIndexTable, "data"), (new Key(rowBytes, family, qualifier, visibility, timestamp.getOrElse(0), false, true), new Value)))
        }

        for (indexValue <- indexColumnUpdates.get(METRIC)) {
          var indexRow = indexValue.getColumnQualifier
          for (storage <- indexColumn.getIndexStorageMethods.asScala.filter(x => x.isInstanceOf[ShardedIndexStorage])) {
            indexRow = storage.encode(indexRow)
          }

          val family = Bytes.concat(indexValue.getColumnFamily, CARDINALITY_CF)
          val qualifier = CARDINALITY_CQ
          val timestamp = if (indexValue.hasTimestamp) Some(indexValue.getTimestamp) else None
          val visibility = indexValue.getColumnVisibility

          keyValues.add(("%s/%s".format(indexColumn.getIndexTable, "data"), (new Key(indexRow, family, qualifier, visibility, timestamp.getOrElse(0), false, true), new Value(encoder.encode(1L)))))
        }
      }
    })

    // Add the row count entry
    if (numRows > 0) {
      val countKey: Key = new Key(METRICS_TABLE_ROW_ID.array(), Bytes.concat(METRICS_TABLE_ROWS_COLUMN.array(), CARDINALITY_CF), CARDINALITY_CQ, EMPTY_BYTES, System.currentTimeMillis(), false, true)
      val countValue: Value = new Value(new FixedLenEncoder().encode(numRows))
      keyValues.add(("%s/%s".format(destTableName + "_metrics", "data"), (countKey, countValue)))
    }

    keyValues.iterator
  }

  private def encodeBytes(ba: Array[Byte]) = {
    val sb = new StringBuilder
    for (b <- ba) {
      val c = 0xff & b
      if (c == '\\') {
        sb.append("\\\\")
      } else if (c >= 32 && c <= 126) {
        sb.append(c.toChar)
      } else {
        sb.append("\\x")
        sb.append(c.toLong.toHexString)
      }
    }
    sb.toString
  }
}

class MultiOutputRDD[K, V](self: RDD[(String, (Key, Value))]) extends Serializable {

  def saveAsMultiTextFiles(path: String) {
    new MultiOutputRDD(self).saveAsNewHadoopMultiOutputs[AccumuloFileOutputFormat](path)
  }

  def saveAsNewHadoopMultiOutputs[F <: OutputFormat[Key, Value]](path: String, conf: Configuration = self.context.hadoopConfiguration)(implicit fm: ClassTag[F]) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val job = new Job(hadoopConf)
    job.setOutputKeyClass(classOf[Key])
    job.setOutputValueClass(classOf[Value])
    LazyOutputFormat.setOutputFormatClass(job, fm.runtimeClass.asInstanceOf[Class[F]])
    job.getConfiguration.set("mapred.output.dir", path)
    saveAsNewAPIHadoopDatasetMultiOutputs(job.getConfiguration)
  }

  def saveAsNewAPIHadoopDatasetMultiOutputs(conf: Configuration) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val job = new Job(hadoopConf)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    val outfmt = job.getOutputFormatClass
    val jobFormat = outfmt.newInstance

    if (conf.getBoolean("spark.hadoop.validateOutputSpecs", true)) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    val writeShard = (context: TaskContext, itr: Iterator[(String, (Key, Value))]) => {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptNumber() % Int.MaxValue).toInt
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = new TaskAttemptID(jobtrackerID, stageId, TaskType.REDUCE, context.partitionId, attemptNumber)
      val hadoopContext = new TaskAttemptContextImpl(wrappedConf.value, attemptId)
      val format = outfmt.newInstance
      format match {
        case c: Configurable => c.setConf(wrappedConf.value)
        case _ => ()
      }
      val committer = format.getOutputCommitter(hadoopContext)

      committer.setupTask(hadoopContext)
      val recordWriter = format.getRecordWriter(hadoopContext).asInstanceOf[RecordWriter[Key, Value]]

      val taskInputOutputContext = new ReduceContextImpl(wrappedConf.value, attemptId, new DummyIterator(itr), new GenericCounter, new GenericCounter,
        recordWriter, committer, new DummyReporter, null, classOf[Key], classOf[Value])
      val writer = new MultipleOutputs(taskInputOutputContext)

      try {
        while (itr.hasNext) {
          val pair = itr.next()
          writer.write(pair._2._1, pair._2._2, pair._1)
        }
      } finally {
        writer.close()
      }
      committer.commitTask(hadoopContext)
      1
    }: Int

    val jobAttemptId = new TaskAttemptID(jobtrackerID, stageId, TaskType.MAP, 0, 0)
    val jobTaskContext = new TaskAttemptContextImpl(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    self.context.runJob(self, writeShard)
    jobCommitter.commitJob(jobTaskContext)
  }

  class DummyIterator(itr: Iterator[_]) extends RawKeyValueIterator {
    def getKey: DataInputBuffer = null

    def getValue: DataInputBuffer = null

    def getProgress: Progress = null

    def next = itr.hasNext

    def close() {}
  }

}

object MultiOutputRDD {
  implicit def rddToMultiOutputRDD[V](rdd: RDD[(String, (Key, Value))]): MultiOutputRDD[Key, Value] = {
    new MultiOutputRDD(rdd)
  }
}