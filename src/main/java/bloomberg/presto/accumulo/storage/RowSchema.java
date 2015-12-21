package bloomberg.presto.accumulo.storage;

import java.util.ArrayList;
import java.util.List;

import bloomberg.presto.accumulo.AccumuloColumnHandle;
import bloomberg.presto.accumulo.PrestoType;
import bloomberg.presto.accumulo.metadata.AccumuloTableMetadataManager;

public class RowSchema {

    private List<AccumuloColumnHandle> columns = new ArrayList<>();

    public static RowSchema newInstance() {
        return new RowSchema();
    }

    public RowSchema addRowId() {
        if (columns.size() != 0) {
            throw new RuntimeException("Row ID must be the first column");
        }

        columns.add(AccumuloTableMetadataManager.getRowIdColumn());
        return this;
    }

    public RowSchema addColumn(String prestoName, String columnFamily,
            String columnQualifier, PrestoType type) {
        columns.add(new AccumuloColumnHandle("accumulo", prestoName,
                columnFamily, columnQualifier, type.spiType(), columns.size(),
                "Accumulo column " + columnFamily + ":" + columnQualifier));
        return this;
    }

    public AccumuloColumnHandle getColumn(int i) {
        return columns.get(i);
    }

    public AccumuloColumnHandle getColumn(String name) {
        for (AccumuloColumnHandle c : columns) {
            if (c.getName().equals(name)) {
                return c;
            }
        }

        throw new RuntimeException("No column with name " + name);
    }

    public List<AccumuloColumnHandle> getColumns() {
        return columns;
    }

    public int getLength() {
        return columns.size();
    }
}
