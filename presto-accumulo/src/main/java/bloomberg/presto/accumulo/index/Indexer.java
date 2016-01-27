package bloomberg.presto.accumulo.index;

import bloomberg.presto.accumulo.AccumuloTable;
import bloomberg.presto.accumulo.Types;
import bloomberg.presto.accumulo.iterators.MaxByteArrayCombiner;
import bloomberg.presto.accumulo.iterators.MinByteArrayCombiner;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.nio.ByteBuffer.wrap;

public class Indexer
        implements Closeable
{
    public static final ByteBuffer METRICS_TABLE_ROW_ID = wrap("METRICS_TABLE".getBytes());
    public static final ByteBuffer METRICS_TABLE_ROWS_CF = wrap("rows".getBytes());
    public static final ByteBuffer METRICS_TABLE_FIRST_ROW_CQ = wrap("first_row".getBytes());
    public static final ByteBuffer METRICS_TABLE_LAST_ROW_CQ = wrap("last_row".getBytes());
    public static final byte[] CARDINALITY_CQ = "cardinality".getBytes();

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte UNDERSCORE = '_';
    private static final TypedValueCombiner.Encoder<Long> ENCODER = new LongCombiner.StringEncoder();

    private final AccumuloTable table;
    private final BatchWriter indexWrtr;
    private final BatchWriterConfig bwc;
    private final Connector conn;
    private final Map<ByteBuffer, Map<ByteBuffer, AtomicLong>> metrics = new HashMap<>();
    private final Map<ByteBuffer, Set<ByteBuffer>> indexColumns = new HashMap<>();
    private final Map<ByteBuffer, Map<ByteBuffer, Type>> indexColumnTypes = new HashMap<>();

    private Comparator<byte[]> byteArrayComparator = UnsignedBytes.lexicographicalComparator();
    private byte[] firstRow = null;
    private byte[] lastRow = null;

    public Indexer(Connector conn, Authorizations auths, AccumuloTable table, BatchWriterConfig bwc)
            throws TableNotFoundException, AccumuloException, AccumuloSecurityException
    {
        this.conn = conn;
        this.table = table;
        this.bwc = bwc;

        // initialize batch writers
        indexWrtr = conn.createBatchWriter(table.getIndexTableName(), bwc);
        table.getColumns().stream().forEach(x -> {
            if (x.isIndexed()) {
                ByteBuffer cf = ByteBuffer.wrap(x.getColumnFamily().getBytes());
                ByteBuffer cq = ByteBuffer.wrap(x.getColumnQualifier().getBytes());

                // add metadata for this column being indexed
                Set<ByteBuffer> qualifiers = indexColumns.get(cf);
                if (qualifiers == null) {
                    qualifiers = new HashSet<>();
                    indexColumns.put(cf, qualifiers);
                }
                qualifiers.add(cq);

                // add metadata for the column type
                Map<ByteBuffer, Type> types = indexColumnTypes.get(cf);
                if (types == null) {
                    types = new HashMap<>();
                    indexColumnTypes.put(cf, types);
                }
                types.put(cq, x.getType());
            }
        });

        if (indexColumns.size() == 0) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "No index columns found in table definition.");
        }

        // initialize metrics map
        Map<ByteBuffer, AtomicLong> cfMap = new HashMap<>();
        cfMap.put(METRICS_TABLE_ROWS_CF, new AtomicLong(0));
        metrics.put(METRICS_TABLE_ROW_ID, cfMap);

        // initialize index columns data structure
        for (AccumuloColumnHandle col : table.getColumns().stream().filter(x -> x.isIndexed()).collect(Collectors.toList())) {
            ByteBuffer cf = wrap(col.getColumnFamily().getBytes());
            Set<ByteBuffer> qualifies = indexColumns.get(cf);
            if (qualifies == null) {
                qualifies = new HashSet<>();
                indexColumns.put(cf, qualifies);
            }
            qualifies.add(wrap(col.getColumnQualifier().getBytes()));
        }

        Scanner scan = conn.createScanner(table.getMetricsTableName(), auths);
        scan.setRange(new Range(new Text(METRICS_TABLE_ROW_ID.array())));
        Text cf = new Text(METRICS_TABLE_ROWS_CF.array());
        Text firstRowCQ = new Text(METRICS_TABLE_FIRST_ROW_CQ.array());
        Text lastRowCQ = new Text(METRICS_TABLE_LAST_ROW_CQ.array());
        scan.fetchColumn(cf, firstRowCQ);
        scan.fetchColumn(cf, lastRowCQ);

        for (Entry<Key, Value> e : scan) {
            if (e.getKey().compareColumnQualifier(firstRowCQ) == 0) {
                firstRow = e.getValue().get();
            }

            if (e.getKey().compareColumnQualifier(lastRowCQ) == 0) {
                lastRow = e.getValue().get();
            }
        }
        scan.close();
    }

    public void index(final Mutation m)
    {
        metrics.get(METRICS_TABLE_ROW_ID).get(METRICS_TABLE_ROWS_CF).incrementAndGet();

        if (firstRow == null || byteArrayComparator.compare(m.getRow(), firstRow) < 0) {
            firstRow = m.getRow();
        }

        if (lastRow == null || byteArrayComparator.compare(m.getRow(), lastRow) > 0) {
            lastRow = m.getRow();
        }

        // for each column update in this mutation
        for (ColumnUpdate cu : m.getUpdates()) {
            // get the column qualifiers we want to index for this column family
            // (if any)
            ByteBuffer cf = wrap(cu.getColumnFamily());
            Set<ByteBuffer> indexCQs = indexColumns.get(cf);

            // if we have column qualifiers we want to index for this column
            // family
            if (indexCQs != null) {
                // check if we want to index this particular qualifier
                ByteBuffer cq = wrap(cu.getColumnQualifier());
                if (indexCQs.contains(cq)) {
                    // Row ID = column value
                    // Column Family = columnqualifier_columnfamily
                    // Column Qualifier = row ID
                    // Value = empty

                    ByteBuffer idxCF = Indexer.getIndexColumnFamily(cu.getColumnFamily(), cu.getColumnQualifier());

                    Type type = indexColumnTypes.get(cf).get(cq);
                    if (Types.isArrayType(type)) {
                        Type eType = Types.getElementType(type);
                        List<?> array = LexicoderRowSerializer.decode(type, cu.getValue());
                        for (Object v : array) {
                            addIndexMutation(wrap(LexicoderRowSerializer.encode(eType, v)), idxCF, m.getRow());
                        }
                    }
                    else {
                        addIndexMutation(wrap(cu.getValue()), idxCF, m.getRow());
                    }
                }
            }
        }
    }

    public void index(Iterable<Mutation> mutations)
    {
        for (Mutation m : mutations) {
            index(m);
        }
    }

    private void addIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier)
    {
        // create the mutation and add it to the given collection
        Mutation mIdx = new Mutation(row.array());
        mIdx.put(family.array(), qualifier, EMPTY_BYTES);
        try {
            indexWrtr.addMutation(mIdx);
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Invalid mutation added to index", e);
        }

        // Increment the metrics for this batch of index mutations
        if (!metrics.containsKey(row)) {
            metrics.put(row, new HashMap<>());
        }

        Map<ByteBuffer, AtomicLong> counter = metrics.get(row);
        if (!counter.containsKey(family)) {
            counter.put(family, new AtomicLong(0));
        }

        counter.get(family).incrementAndGet();
    }

    public void flush()
    {
        try {
            // flush index writer
            indexWrtr.flush();

            // write out metrics mutations
            BatchWriter metricsWrtr = conn.createBatchWriter(table.getMetricsTableName(), bwc);
            metricsWrtr.addMutations(getMetricsMutations());
            metricsWrtr.close();

            // re-initialize the metrics
            metrics.clear();
            Map<ByteBuffer, AtomicLong> cfMap = new HashMap<>();
            cfMap.put(METRICS_TABLE_ROWS_CF, new AtomicLong(0));
            metrics.put(METRICS_TABLE_ROW_ID, cfMap);
        }
        catch (MutationsRejectedException | TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Invalid mutation added to index metrics", e);
        }
    }

    @Override
    public void close()
    {
        try {
            flush();
            indexWrtr.close();
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
    }

    private Collection<Mutation> getMetricsMutations()
    {
        List<Mutation> muts = new ArrayList<>();
        for (Entry<ByteBuffer, Map<ByteBuffer, AtomicLong>> m : metrics.entrySet()) {
            ByteBuffer idxRow = m.getKey();
            // create new mutation
            Mutation mut = new Mutation(idxRow.array());
            for (Entry<ByteBuffer, AtomicLong> columnValues : m.getValue().entrySet()) {
                mut.put(columnValues.getKey().array(), CARDINALITY_CQ, ENCODER.encode(columnValues.getValue().get()));
            }
            muts.add(mut);
        }

        if (firstRow != null && lastRow != null) {
            Mutation flm = new Mutation(METRICS_TABLE_ROW_ID.array());
            flm.put(METRICS_TABLE_ROWS_CF.array(), METRICS_TABLE_FIRST_ROW_CQ.array(), firstRow);
            flm.put(METRICS_TABLE_ROWS_CF.array(), METRICS_TABLE_LAST_ROW_CQ.array(), lastRow);
            muts.add(flm);
        }

        return muts;
    }

    public static Collection<IteratorSetting> getMetricIterators(AccumuloTable table)
    {
        String cardCq = new String(CARDINALITY_CQ);
        String rowsCf = new String(METRICS_TABLE_ROWS_CF.array());
        StringBuilder cardBldr = new StringBuilder(rowsCf + ":" + cardCq + ",");
        for (String s : getLocalityGroups(table).keySet()) {
            cardBldr.append(s).append(":").append(cardCq).append(',');
        }
        cardBldr.deleteCharAt(cardBldr.length() - 1);

        String firstRowColumn = rowsCf + ":" + new String(METRICS_TABLE_FIRST_ROW_CQ.array());
        String lastRowColumn = rowsCf + ":" + new String(METRICS_TABLE_LAST_ROW_CQ.array());

        IteratorSetting s1 = new IteratorSetting(1, SummingCombiner.class, ImmutableMap.of("columns", cardBldr.toString(), "type", "STRING"));
        IteratorSetting s2 = new IteratorSetting(2, MinByteArrayCombiner.class, ImmutableMap.of("columns", firstRowColumn));
        IteratorSetting s3 = new IteratorSetting(3, MaxByteArrayCombiner.class, ImmutableMap.of("columns", lastRowColumn));

        return ImmutableList.of(s1, s2, s3);
    }

    public static ByteBuffer getIndexColumnFamily(byte[] columnFamily, byte[] columnQualifier)
    {
        return wrap(ArrayUtils.addAll(ArrayUtils.add(columnFamily, UNDERSCORE), columnQualifier));
    }

    public static Map<String, Set<Text>> getLocalityGroups(AccumuloTable table)
    {
        Map<String, Set<Text>> groups = new HashMap<>();
        for (AccumuloColumnHandle acc : table.getColumns().stream().filter(x -> x.isIndexed()).collect(Collectors.toList())) {
            Text indexColumnFamily = new Text(acc.getColumnFamily() + "_" + acc.getColumnQualifier());
            groups.put(indexColumnFamily.toString(), ImmutableSet.of(indexColumnFamily));
        }
        return groups;
    }

    public static String getIndexTableName(String schema, String table)
    {
        return schema.equals("default") ? table + "_idx" : schema + '.' + table + "_idx";
    }

    public static String getIndexTableName(SchemaTableName stName)
    {
        return getIndexTableName(stName.getSchemaName(), stName.getTableName());
    }

    public static String getMetricsTableName(String schema, String table)
    {
        return schema.equals("default") ? table + "_idx_metrics" : schema + '.' + table + "_idx_metrics";
    }

    public static String getMetricsTableName(SchemaTableName stName)
    {
        return getMetricsTableName(stName.getSchemaName(), stName.getTableName());
    }
}
