package bloomberg.presto.accumulo;

import bloomberg.presto.accumulo.index.Indexer;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AccumuloPageSink
        implements ConnectorPageSink
{
    private final AccumuloRowSerializer serializer;
    private final BatchWriter wrtr;
    private final List<AccumuloColumnHandle> types;
    private final Indexer indexer;
    private final List<Row> rows = new ArrayList<>();
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
            BatchWriterConfig conf = new BatchWriterConfig();
            wrtr = conn.createBatchWriter(table.getFullTableName(), conf);

            if (table.isIndexed()) {
                indexer = new Indexer(conn, table, conf);
            }
            else {
                indexer = null;
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

                    if (indexer != null) {
                        indexer.index(m);
                    }
                }
                else {
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "At least one non-recordkey column must contain a non-null value");
                }
            }

            wrtr.close();

            if (indexer != null) {
                indexer.close();
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
        // make a new mutation, passing in the row ID
        Text rowId = new Text();
        for (AccumuloColumnHandle ach : columns) {
            if (ach.getName().equals(rowIdName)) {
                if (row.getField(ach.getOrdinal()).isNull()) {
                    throw new PrestoException(StandardErrorCode.USER_ERROR, "Row recordkey cannot be null");
                }

                setText(serializer, rowId, ach.getType(), row.getField(ach.getOrdinal()));
            }
        }

        if (rowId.getLength() == 0) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to locate row ID in columns");
        }

        Mutation m = new Mutation(rowId);
        Text value = new Text();
        for (AccumuloColumnHandle ach : columns) {
            if (!row.getField(ach.getOrdinal()).isNull() && !ach.getName().equals(rowIdName)) {
                setText(serializer, value, ach.getType(), row.getField(ach.getOrdinal()));
                m.put(ach.getColumnFamily(), ach.getColumnQualifier(), new Value(value.copyBytes()));
            }
        }

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
