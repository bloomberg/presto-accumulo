package bloomberg.presto.accumulo.model;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to define the schema of a Row, stored as a list of {@link AccumuloColumnHandle}.
 */
public class RowSchema
{
    private List<AccumuloColumnHandle> columns = new ArrayList<>();

    /**
     * Gets a new instance of RowSchema.
     *
     * @return A new RowSchema
     */
    public static RowSchema newRowSchema()
    {
        return new RowSchema();
    }

    /**
     * Adds the Row ID column to the schema
     *
     * @return this, for chaining
     */
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

    /**
     * Appends a new non-indexed column to the end of the schema.
     *
     * @param prestoName
     *            Presto column name
     * @param family
     *            Accumulo column family
     * @param qualifier
     *            Accumulo column qualifier
     * @param type
     *            Presto type of the column
     * @return this, for schema
     */
    public RowSchema addColumn(String prestoName, String family, String qualifier, Type type)
    {
        return addColumn(prestoName, family, qualifier, type, false);
    }

    /**
     * Appends a new column to the end of the schema.
     *
     * @param prestoName
     *            Presto column name
     * @param family
     *            Accumulo column family
     * @param qualifier
     *            Accumulo column qualifier
     * @param type
     *            Presto type of the column
     * @param indexed
     *            True if indexed, false otherwise
     * @return this, for schema
     */
    public RowSchema addColumn(String prestoName, String family, String qualifier, Type type,
            boolean indexed)
    {
        columns.add(new AccumuloColumnHandle("accumulo", prestoName, family, qualifier, type,
                columns.size(),
                String.format("Accumulo column %s:%s. Indexed: %b", family, qualifier, indexed),
                indexed));
        return this;
    }

    /**
     * Gets the column handle at the given zero-based indexed
     *
     * @param i
     *            Index to retrieve
     * @return The column handle
     * @throws IndexOutOfBoundsException
     *             If the index is larger than the length
     */
    public AccumuloColumnHandle getColumn(int i)
    {
        return columns.get(i);
    }

    /**
     * Gets the column handle that matches the given Presto column name
     *
     * @param name
     *            Presto column name
     * @return The column handle
     * @throws PrestoException
     *             If the column is not found
     */
    public AccumuloColumnHandle getColumn(String name)
    {
        for (AccumuloColumnHandle c : columns) {
            if (c.getName().equals(name)) {
                return c;
            }
        }

        throw new PrestoException(StandardErrorCode.USER_ERROR, "No column with name " + name);
    }

    /**
     * Gets all column handles in the schema
     *
     * @return Column handle
     */
    public List<AccumuloColumnHandle> getColumns()
    {
        return columns;
    }

    /**
     * Gets the length of the schema, i.e. number of fields
     *
     * @return Length of the schema
     */
    public int getLength()
    {
        return columns.size();
    }
}
