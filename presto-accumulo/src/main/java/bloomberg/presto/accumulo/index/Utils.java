package bloomberg.presto.accumulo.index;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.lang.ArrayUtils;

import bloomberg.presto.accumulo.model.AccumuloColumnHandle;

public class Utils {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte UNDERSCORE = '_';

    public static void indexMutation(final Mutation m,
            final Map<ByteBuffer, Set<ByteBuffer>> indexColumns,
            final Collection<Mutation> updates) {

        // for each column update in this mutation
        for (ColumnUpdate cu : m.getUpdates()) {

            // get the column qualifiers we want to index for this column family
            // (if any)
            ByteBuffer cf = ByteBuffer.wrap(cu.getColumnFamily());
            Set<ByteBuffer> indexCQs = indexColumns.get(cf);

            // if we have column qualifiers we want to index for this column
            // family
            if (indexCQs != null) {
                // check if we want to index this particular qualifier
                ByteBuffer cq = ByteBuffer.wrap(cu.getColumnQualifier());
                if (indexCQs.contains(cq)) {
                    // Row ID = column value
                    // Column Family = columnqualifier_columnfamily
                    // Column Qualifier = row ID
                    // Value = empty

                    // create the mutation and add it to the given collection
                    Mutation mIdx = new Mutation(cu.getValue());
                    mIdx.put(ArrayUtils.addAll(
                            ArrayUtils.add(cu.getColumnFamily(), UNDERSCORE),
                            cu.getColumnQualifier()), m.getRow(), EMPTY_BYTES);

                    updates.add(mIdx);
                }
            }
        }
    }

    public static Map<ByteBuffer, Set<ByteBuffer>> getMapOfIndexedColumns(
            List<AccumuloColumnHandle> columns) {
        Map<ByteBuffer, Set<ByteBuffer>> indexColumns = new HashMap<>();

        for (AccumuloColumnHandle col : columns.stream()
                .filter(x -> x.isIndexed()).collect(Collectors.toList())) {
            ByteBuffer cf = ByteBuffer.wrap(col.getColumnFamily().getBytes());
            Set<ByteBuffer> qualifies = indexColumns.get(cf);
            if (qualifies == null) {
                qualifies = new HashSet<>();
                indexColumns.put(cf, qualifies);
            }
            qualifies.add(ByteBuffer.wrap(col.getColumnQualifier().getBytes()));
        }

        return indexColumns;
    }
}
