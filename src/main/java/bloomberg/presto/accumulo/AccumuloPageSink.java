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
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

import bloomberg.presto.accumulo.io.AccumuloRowSerializer;
import bloomberg.presto.accumulo.metadata.AccumuloTableMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.model.Row;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

public class AccumuloPageSink implements ConnectorPageSink {
    private static final Logger LOG = Logger.get(AccumuloPageSink.class);

    private final BatchWriter wrtr;
    private final List<Row> rows = new ArrayList<>();
    private final List<AccumuloColumnHandle> types;
    private final AccumuloRowSerializer serializer;

    public AccumuloPageSink(Connector conn, AccumuloTable table,
            AccumuloRowSerializer serializer) {
        requireNonNull(conn, "conn is null");
        requireNonNull(table, "tHandle is null");
        this.serializer = requireNonNull(serializer, "serializer is null");
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
                wrtr.addMutation(toMutation(row, types, serializer));
            }
            wrtr.close();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
        return ImmutableList.of();
    }

    @Override
    public void rollback() {
        LOG.debug("rollback");
    }

    public static Mutation toMutation(Row row,
            List<AccumuloColumnHandle> columns,
            AccumuloRowSerializer serializer) {
        // make a new mutation, passing in the row ID
        Mutation m = new Mutation(row.getField(0).getValue().toString());
    
        Text cf = new Text(), cq = new Text(), value = new Text();
        // for each column in the input schema
        for (int i = 1; i < columns.size(); ++i) {
            AccumuloColumnHandle ach = columns.get(i);
            // if this column's name is not the row ID
            if (!ach.getName()
                    .equals(AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME)) {
                switch (PrestoType.fromSpiType(ach.getType())) {
                case BIGINT:
                    serializer.setLong(value, row.getField(i).getBigInt());
                    break;
                case BOOLEAN:
                    serializer.setBoolean(value, row.getField(i).getBoolean());
                    break;
                case DATE:
                    serializer.setDate(value, row.getField(i).getDate());
                    break;
                case DOUBLE:
                    serializer.setDouble(value, row.getField(i).getDouble());
                    break;
                case TIME:
                    serializer.setTime(value, row.getField(i).getTime());
                    break;
                case TIMESTAMP:
                    serializer.setTimestamp(value,
                            row.getField(i).getTimestamp());
                    break;
                case VARBINARY:
                    serializer.setVarbinary(value,
                            row.getField(i).getVarbinary());
                    break;
                case VARCHAR:
                    serializer.setVarchar(value, row.getField(i).getVarchar());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported type " + ach.getType());
                }
    
                cf.set(ach.getColumnFamily());
                cq.set(ach.getColumnQualifier());
                m.put(cf, cq, new Value(value.copyBytes()));
            }
        }
    
        return m;
    }
}
