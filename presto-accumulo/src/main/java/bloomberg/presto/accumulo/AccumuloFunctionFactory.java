package bloomberg.presto.accumulo;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

/**
 * An implementation of a FunctionFactory to provide additional UDF functionality for Presto
 *
 * @see AccumuloStringFunctions
 */
public class AccumuloFunctionFactory
        implements FunctionFactory
{
    private final TypeManager typeManager;

    /**
     * Creates a new instance of {@link AccumuloFunctionFactory}
     *
     * @param typeManager
     *            Type manager
     */
    public AccumuloFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    /**
     * Lists all SqlFunctions provided by this connector
     *
     * @see AccumuloStringFunctions
     * @return A list of SQL functions
     */
    @Override
    public List<SqlFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager).scalar(AccumuloStringFunctions.class)
                .getFunctions();
    }
}
