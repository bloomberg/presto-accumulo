package bloomberg.presto.accumulo.benchmark;

import java.util.ArrayList;
import java.util.List;

import bloomberg.presto.accumulo.AccumuloColumnMetadataProvider;
import bloomberg.presto.accumulo.PrestoType;

public class RowSchema {

    private List<Column> columns = new ArrayList<>();

    public static RowSchema newInstance() {
        return new RowSchema();
    }

    public RowSchema addRowId() {
        columns.add(
                new Column(AccumuloColumnMetadataProvider.ROW_ID_COLUMN_NAME,
                        null, null, PrestoType.VARCHAR, columns.size()));
        return this;
    }

    public RowSchema addColumn(String prestoName, String columnFamily,
            String columnQualifier, PrestoType type) {
        columns.add(new Column(prestoName, columnFamily, columnQualifier, type,
                columns.size()));
        return this;
    }

    public Column getColumn(int i) {
        return columns.get(i);
    }

    public Column getColumn(String name) {
        for (Column c : columns) {
            if (c.getPrestoName().equals(name)) {
                return c;
            }
        }

        throw new RuntimeException("No column with name " + name);
    }

    public List<Column> getColumns() {
        return columns;
    }

    public int getLength() {
        return columns.size();
    }
}
