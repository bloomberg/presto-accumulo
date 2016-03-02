package bloomberg.presto.accumulo.model;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;

public class RowSchema
{
    private List<AccumuloColumnHandle> columns = new ArrayList<>();

    public static RowSchema newInstance()
    {
        return new RowSchema();
    }

    public RowSchema addRowId()
    {
        // TODO Fix this! Row ID doesn't have to be first anymore. Nope, not anymore.
        if (columns.size() != 0) {
            throw new PrestoException(StandardErrorCode.USER_ERROR,
                    "Row ID must be the first column");
        }

        columns.add(new AccumuloColumnHandle("accumulo", "recordkey", null, null,
                VarcharType.VARCHAR, columns.size(), "Accumulo row ID", false));
        return this;
    }

    public RowSchema addColumn(String prestoName, String columnFamily, String columnQualifier,
            Type type)
    {
        return addColumn(prestoName, columnFamily, columnQualifier, type, false);
    }

    public RowSchema addColumn(String prestoName, String columnFamily, String columnQualifier,
            Type type, boolean indexed)
    {
        columns.add(new AccumuloColumnHandle("accumulo", prestoName, columnFamily, columnQualifier,
                type, columns.size(), String.format("Accumulo column %s:%s. Indexed: %b",
                        columnFamily, columnQualifier, indexed),
                indexed));
        return this;
    }

    public AccumuloColumnHandle getColumn(int i)
    {
        return columns.get(i);
    }

    public AccumuloColumnHandle getColumn(String name)
    {
        for (AccumuloColumnHandle c : columns) {
            if (c.getName().equals(name)) {
                return c;
            }
        }

        throw new PrestoException(StandardErrorCode.USER_ERROR, "No column with name " + name);
    }

    public List<AccumuloColumnHandle> getColumns()
    {
        return columns;
    }

    public int getLength()
    {
        return columns.size();
    }
}
