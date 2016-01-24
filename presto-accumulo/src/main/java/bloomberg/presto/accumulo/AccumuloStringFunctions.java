package bloomberg.presto.accumulo;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.UUID;

public class AccumuloStringFunctions
{
    private AccumuloStringFunctions()
    {}

    @Description("Returns a randomly generated UUID")
    @ScalarFunction("uuid")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice UUID()
    {
        return Slices.utf8Slice(UUID.randomUUID().toString());
    }
}
