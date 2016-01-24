package bloomberg.presto.accumulo;

import bloomberg.presto.accumulo.index.Utils;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.model.Field;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class AccumuloPageSink
        implements ConnectorPageSink
{
    private final AccumuloRowSerializer serializer;
    private final BatchWriter wrtr;
    private final BatchWriter indexWrtr;
    private final BatchWriter metricsWrtr;
    private final List<AccumuloColumnHandle> types;
    private final List<Mutation> idxMutations = new ArrayList<>();
    private final List<Row> rows = new ArrayList<>();
    private final Map<ByteBuffer, Set<ByteBuffer>> indexColumns = new HashMap<>();
    private final Map<ByteBuffer, Map<ByteBuffer, AtomicLong>> metrics;
    private final String rowIdName;

    public AccumuloPageSink(Connector conn, AccumuloTable table)
    {
        requireNonNull(conn, "conn is null");
        requireNonNull(table, "tHandle is null");
        try {
            this.serializer = table.getSerializerClass().newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to factory serializer class", e);
        }

        this.types = table.getColumns();

        try {
            wrtr = conn.createBatchWriter(table.getFullTableName(), new BatchWriterConfig());

            if (table.isIndexed()) {
                indexWrtr = conn.createBatchWriter(table.getIndexTableName(), new BatchWriterConfig());
                table.getColumns().stream().forEach(x -> {
                    if (x.isIndexed()) {
                        ByteBuffer cf = ByteBuffer.wrap(x.getColumnFamily().getBytes());
                        ByteBuffer cq = ByteBuffer.wrap(x.getColumnQualifier().getBytes());
                        Set<ByteBuffer> qualifiers = indexColumns.get(cf);
                        if (qualifiers == null) {
                            qualifiers = new HashSet<>();
                            indexColumns.put(cf, qualifiers);
                        }
                        qualifiers.add(cq);
                    }
                });

                metricsWrtr = conn.createBatchWriter(table.getMetricsTableName(), new BatchWriterConfig());
                metrics = Utils.getMetricsDataStructure();
            }
            else {
                indexWrtr = null;
                metricsWrtr = null;
                metrics = null;
            }
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }

        rowIdName = table.getRowIdName();
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        for (int position = 0; position < page.getPositionCount(); ++position) {
            Row r = Row.newInstance();
            for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                Type type = types.get(channel).getType();
                r.addField(TypeUtils.readNativeValue(type, page.getBlock(channel), position), type);
            }
            rows.add(r);
        }
    }

    @Override
    public Collection<Slice> commit()
    {
        try {
            for (Row row : rows) {
                Mutation m = toMutation(row, rowIdName, types, serializer);
                if (m.size() > 0) {
                    wrtr.addMutation(m);

                    if (indexWrtr != null) {
                        Utils.indexMutation(m, indexColumns, idxMutations, metrics);
                        indexWrtr.addMutations(idxMutations);
                        idxMutations.clear();
                    }
                }
                else {
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "At least one non-recordkey column must contain a non-null value");
                }
            }

            wrtr.close();

            if (indexWrtr != null) {
                indexWrtr.close();
                metricsWrtr.addMutations(Utils.getMetricsMutations(metrics));
                metricsWrtr.close();
            }
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
        return ImmutableList.of();
    }

    @Override
    public void rollback()
    {}

    public static Mutation toMutation(Row row, String rowIdName, List<AccumuloColumnHandle> columns, AccumuloRowSerializer serializer)
    {
        if (row.getField(0).isNull()) {
            throw new PrestoException(StandardErrorCode.USER_ERROR, "Row recordkey cannot be null");
        }

        // make a new mutation, passing in the row ID
        Text rowId = new Text();
        columns.parallelStream().filter(x -> x.getName().equals(rowIdName)).forEach(x -> setText(serializer, rowId, x.getType(), row.getField(x.getOrdinal())));

        if (rowId.getLength() == 0) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to locate row ID in columns");
        }

        Mutation m = new Mutation(rowId);

        Text value = new Text();
        columns.stream().filter(x -> !row.getField(x.getOrdinal()).isNull() && !x.getName().equals(rowIdName)).forEach(ach -> {
            setText(serializer, value, ach.getType(), row.getField(ach.getOrdinal()));
            m.put(ach.getColumnFamily(), ach.getColumnQualifier(), new Value(value.copyBytes()));
        });

        return m;
    }

    private static void setText(AccumuloRowSerializer serializer, Text value, Type type, Field field)
    {
        if (Types.isArrayType(type)) {
            serializer.setArray(value, type, field.getBlock());
        }
        else if (Types.isMapType(type)) {
            serializer.setMap(value, type, field.getBlock());
        }
        else {
            switch (type.getDisplayName()) {
                case StandardTypes.BIGINT:
                    serializer.setLong(value, field.getBigInt());
                    break;
                case StandardTypes.BOOLEAN:
                    serializer.setBoolean(value, field.getBoolean());
                    break;
                case StandardTypes.DATE:
                    serializer.setDate(value, field.getDate());
                    break;
                case StandardTypes.DOUBLE:
                    serializer.setDouble(value, field.getDouble());
                    break;
                case StandardTypes.TIME:
                    serializer.setTime(value, field.getTime());
                    break;
                case StandardTypes.TIMESTAMP:
                    serializer.setTimestamp(value, field.getTimestamp());
                    break;
                case StandardTypes.VARBINARY:
                    serializer.setVarbinary(value, field.getVarbinary());
                    break;
                case StandardTypes.VARCHAR:
                    serializer.setVarchar(value, field.getVarchar());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type " + type);
            }
        }
    }
}
