package bloomberg.presto.accumulo;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

public class AccumuloFunctionFactory
        implements FunctionFactory
{
    private final TypeManager typeManager;

    public AccumuloFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public List<SqlFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager).scalar(AccumuloStringFunctions.class).getFunctions();
    }
}
