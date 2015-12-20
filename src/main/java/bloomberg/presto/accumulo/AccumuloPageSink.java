package bloomberg.presto.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

import bloomberg.presto.accumulo.metadata.AccumuloTableMetadataManager;
import bloomberg.presto.accumulo.storage.Row;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

public class AccumuloPageSink implements ConnectorPageSink {
    private static final Logger LOG = Logger.get(AccumuloPageSink.class);

    private final BatchWriter wrtr;
    private final List<Row> rows = new ArrayList<>();
    private final List<AccumuloColumnHandle> types;

    public AccumuloPageSink(Connector conn, AccumuloTable table) {
        requireNonNull(conn, "conn is null");
        requireNonNull(table, "tHandle is null");
        this.types = table.getColumns();

        try {
            wrtr = conn.createBatchWriter(table.getName(),
                    new BatchWriterConfig());
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock) {
        LOG.debug("appendPage " + page + " " + sampleWeightBlock
                + " num blocks " + page.getBlocks().length);

        for (int position = 0; position < page.getPositionCount(); ++position) {
            Row r = Row.newInstance();
            for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                Type type = types.get(channel).getType();
                r.addField(getNativeContainerValue(type, page.getBlock(channel),
                        position), PrestoType.fromSpiType(type));
            }
            rows.add(r);
        }
    }

    private static Object getNativeContainerValue(Type type, Block block,
            int position) {
        if (block.isNull(position)) {
            return null;
        } else if (type.getJavaType() == boolean.class) {
            return type.getBoolean(block, position);
        } else if (type.getJavaType() == long.class) {
            return type.getLong(block, position);
        } else if (type.getJavaType() == double.class) {
            return type.getDouble(block, position);
        } else if (type.getJavaType() == Slice.class) {
            Slice slice = (Slice) type.getSlice(block, position);
            return type.equals(VarcharType.VARCHAR) ? slice.toStringUtf8()
                    : slice.getBytes();
        } else {
            throw new AssertionError("Unimplemented type: " + type);
        }
    }

    @Override
    public Collection<Slice> commit() {

        try {
            for (Row row : rows) {
                wrtr.addMutation(toMutation(row, types));
            }
            wrtr.close();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
        return ImmutableList.of();
    }

    public static Mutation toMutation(Row row,
            List<AccumuloColumnHandle> columns) {
        // make a new mutation, passing in the row ID
        Mutation m = new Mutation(row.getField(0).getValue().toString());

        // for each column in the input schema
        for (int i = 1; i < columns.size(); ++i) {
            AccumuloColumnHandle ach = columns.get(i);
            // if this column's name is not the row ID
            if (!ach.getName()
                    .equals(AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME)) {
                switch (PrestoType.fromSpiType(ach.getType())) {
                case DATE:
                    m.put(ach.getColumnFamily(), ach.getColumnQualifier(),
                            Long.toString(row.getField(i).getDate().getTime()));
                    break;
                case TIME:
                    m.put(ach.getColumnFamily(), ach.getColumnQualifier(),
                            Long.toString(row.getField(i).getTime().getTime()));
                    break;
                case TIMESTAMP:
                    m.put(ach.getColumnFamily(), ach.getColumnQualifier(),
                            Long.toString(
                                    row.getField(i).getTimestamp().getTime()));
                    break;
                case VARBINARY:
                    m.put(ach.getColumnFamily(), ach.getColumnQualifier(),
                            new Value(row.getField(i).getVarBinary()));
                    break;
                default:
                    m.put(ach.getColumnFamily(), ach.getColumnQualifier(),
                            row.getField(i).getValue().toString());
                    break;
                }
            }
        }

        return m;
    }

    @Override
    public void rollback() {
        LOG.debug("rollback");
    }
}
