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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.google.common.collect.ImmutableList;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

public class AccumuloPageSink implements ConnectorPageSink {
    private static final Logger LOG = Logger.get(AccumuloPageSink.class);

    private final BatchWriter wrtr;
    private final List<Row> rows = new ArrayList<>();
    private final List<AccumuloColumnHandle> types;
    private final AccumuloRowSerializer serializer;

    public AccumuloPageSink(Connector conn, AccumuloTable table) {
        requireNonNull(conn, "conn is null");
        requireNonNull(table, "tHandle is null");
        try {
            this.serializer = table.getSerializerClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to factory serializer class", e);
        }

        this.types = table.getColumns();

        try {
            wrtr = conn.createBatchWriter(table.getFullTableName(),
                    new BatchWriterConfig());
        } catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
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
                r.addField(TypeUtils.readNativeValue(type,
                        page.getBlock(channel), position), type);
            }
            rows.add(r);
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
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
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
        Mutation m = new Mutation(row.getField(0).getObject().toString());

        Text cf = new Text(), cq = new Text(), value = new Text();
        // for each column in the input schema
        for (int i = 1; i < columns.size(); ++i) {
            AccumuloColumnHandle ach = columns.get(i);
            // if this column's name is not the row ID
            if (!ach.getName()
                    .equals(AccumuloMetadataManager.ROW_ID_COLUMN_NAME)) {

                if (row.getField(i).isNull()) {
                    continue;
                }

                if (Types.isArrayType(ach.getType())) {
                    serializer.setArray(value, ach.getType(),
                            row.getField(i).getBlock());
                } else if (Types.isMapType(ach.getType())) {
                    serializer.setMap(value, ach.getType(),
                            row.getField(i).getBlock());
                } else {
                    switch (ach.getType().getDisplayName()) {
                    case StandardTypes.BIGINT:
                        serializer.setLong(value, row.getField(i).getBigInt());
                        break;
                    case StandardTypes.BOOLEAN:
                        serializer.setBoolean(value,
                                row.getField(i).getBoolean());
                        break;
                    case StandardTypes.DATE:
                        serializer.setDate(value, row.getField(i).getDate());
                        break;
                    case StandardTypes.DOUBLE:
                        serializer.setDouble(value,
                                row.getField(i).getDouble());
                        break;
                    case StandardTypes.TIME:
                        serializer.setTime(value, row.getField(i).getTime());
                        break;
                    case StandardTypes.TIMESTAMP:
                        serializer.setTimestamp(value,
                                row.getField(i).getTimestamp());
                        break;
                    case StandardTypes.VARBINARY:
                        serializer.setVarbinary(value,
                                row.getField(i).getVarbinary());
                        break;
                    case StandardTypes.VARCHAR:
                        serializer.setVarchar(value,
                                row.getField(i).getVarchar());
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported type " + ach.getType());
                    }
                }

                cf.set(ach.getColumnFamily());
                cq.set(ach.getColumnQualifier());
                m.put(cf, cq, new Value(value.copyBytes()));
            }
        }

        return m;
    }
}
