package bloomberg.presto.accumulo.io;

import bloomberg.presto.accumulo.Types;
import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.index.Indexer;
import bloomberg.presto.accumulo.metadata.AccumuloTable;
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
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
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

/**
 * Output class for serializing Presto pages (blocks of rows of data) to Accumulo. This class
 * converts every page to an in-memory list of rows, then flushes them all to memory. In the event
 * of an error, the transaction is rolled back, which is effectively a no-op.
 *
 * TODO Probably should just write them out to the table instead of batching them in memory. Who
 * knows how big these pages are. Not strictly a rhetorical question.
 *
 * @see AccumuloPageSinkProvider
 */
public class AccumuloPageSink
        implements ConnectorPageSink
{
    private final AccumuloRowSerializer serializer;
    private final BatchWriter wrtr;
    private final Indexer indexer;
    private final List<AccumuloColumnHandle> columns;
    private final List<Row> rows = new ArrayList<>();
    private Integer rowIdOrdinal;

    /**
     * Creates a new instance of {@link AccumuloPageSink}
     *
     * @param conn
     *            Connector for Accumulo
     * @param accConfig
     *            Configuration for Accumulo
     * @param table
     *            Table metadata for an Accumulo table
     */
    public AccumuloPageSink(Connector conn, AccumuloConfig accConfig, AccumuloTable table)
    {
        requireNonNull(conn, "conn is null");
        requireNonNull(accConfig, "accConfig is null");
        requireNonNull(table, "table is null");

        this.columns = table.getColumns();

        // Fetch the row ID ordinal, throwing an exception if not found for safety
        for (AccumuloColumnHandle ach : columns) {
            if (ach.getName().equals(table.getRowId())) {
                rowIdOrdinal = ach.getOrdinal();
                break;
            }
        }

        if (rowIdOrdinal == null) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Row ID ordinal not found");
        }

        try {
            this.serializer = table.getSerializerClass().newInstance();
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Failed to factory table serializer from configured class", e);
        }

        try {
            // Create a BatchWriter to the Accumulo table
            BatchWriterConfig conf = new BatchWriterConfig();
            wrtr = conn.createBatchWriter(table.getFullTableName(), conf);

            // If the table is indexed, create an instance of an Indexer, else null
            if (table.isIndexed()) {
                indexer = new Indexer(conn,
                        conn.securityOperations().getUserAuthorizations(accConfig.getUsername()),
                        table, conf);
            }
            else {
                indexer = null;
            }
        }
        catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Accumulo error when creating BatchWriter and/or Indexer", e);
        }
    }

    /**
     * Appends a page
     */
    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        // For each position within the page, i.e. row
        for (int position = 0; position < page.getPositionCount(); ++position) {
            Row r = Row.newRow();
            rows.add(r);
            // For each channel within the page, i.e. column
            for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                // Get the type for this channel
                Type type = columns.get(channel).getType();

                // Read the value from the page and append the field to the row
                r.addField(TypeUtils.readNativeValue(type, page.getBlock(channel), position), type);
            }
        }
    }

    /**
     * Commit the insert to Accumulo, flushing out all rows
     */
    @Override
    public Collection<Slice> commit()
    {
        try {
            // Convert each Row into a Mutation
            for (Row row : rows) {
                Mutation m = toMutation(row, rowIdOrdinal, columns, serializer);

                // If this mutation has columns
                if (m.size() > 0) {
                    // Write the mutation and index it
                    wrtr.addMutation(m);

                    if (indexer != null) {
                        indexer.index(m);
                    }
                }
                else {
                    // Else, this Mutation contains only a row ID and will throw an exception if
                    // added so, we throw one here with a more descriptive message!
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                            "At least one non-recordkey column must contain a non-null value");
                }
            }

            // Done serializing rows, so flush and close the writer and indexer
            wrtr.flush();
            wrtr.close();
            if (indexer != null) {
                indexer.close();
            }
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }

        // TODO Look into any use of the metadata for writing out the rows
        return ImmutableList.of();
    }

    /**
     * Rollback the page appending
     */
    @Override
    public void rollback()
    {
        rows.clear();
    }

    /**
     * Converts a {@link Row} to an Accumulo mutation.
     *
     * @param row
     *            Row object
     * @param rowIdOrdinal
     *            Ordinal in the list of columns that is the row ID. This isn't checked at all, so I
     *            hope you're right. Also, it is expected that the list of column handles is sorted
     *            in ordinal order. This is a very demanding function.
     * @param columns
     *            All column handles for the Row, sorted by ordinal.
     * @param serializer
     *            Instance of {@link AccumuloRowSerializer} used to encode the values of the row to
     *            the Mutation
     * @return Mutation
     */
    public static Mutation toMutation(Row row, int rowIdOrdinal, List<AccumuloColumnHandle> columns,
            AccumuloRowSerializer serializer)
    {
        // Set our value to the row ID
        Text value = new Text();
        setText(row.getField(rowIdOrdinal), value, serializer);

        // Iterate through all the column handles, setting the Mutation's columns
        Mutation m = new Mutation(value);
        for (AccumuloColumnHandle ach : columns) {
            // Skip the row ID ordinal
            if (ach.getOrdinal() == rowIdOrdinal) {
                continue;
            }

            // If the value of the field is not null
            if (!row.getField(ach.getOrdinal()).isNull()) {
                // Serialize the value to the text
                setText(row.getField(ach.getOrdinal()), value, serializer);

                // And add the bytes to the Mutation
                m.put(ach.getFamily(), ach.getQualifier(), new Value(value.copyBytes()));
            }
        }

        return m;
    }

    /**
     * Sets the value of the given Text object to the encoded value of the given field.
     *
     * @param field
     *            Value of the field to encode
     * @param value
     *            Text object to set
     * @param serializer
     *            Serializer to use to encode the value
     */
    private static void setText(Field field, Text value, AccumuloRowSerializer serializer)
    {
        Type type = field.getType();
        if (Types.isArrayType(type)) {
            serializer.setArray(value, type, field.getArray());
        }
        else if (Types.isMapType(type)) {
            serializer.setMap(value, type, field.getMap());
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
