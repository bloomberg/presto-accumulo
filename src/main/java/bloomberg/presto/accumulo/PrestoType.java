package bloomberg.presto.accumulo;

import java.security.InvalidParameterException;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;

/**
 * An enum wrapper for Presto's {@link StandardTypes}, because enums are much
 * easier to work with for this kind of stuff
 */
public enum PrestoType {

    BIGINT(StandardTypes.BIGINT), BOOLEAN(StandardTypes.BOOLEAN), DATE(
            StandardTypes.DATE), DOUBLE(StandardTypes.DOUBLE), TIME(
                    StandardTypes.TIME), TIMESTAMP(
                            StandardTypes.TIMESTAMP), VARBINARY(
                                    StandardTypes.VARBINARY), VARCHAR(
                                            StandardTypes.VARCHAR);

    private String str;

    private PrestoType(String str) {
        this.str = str;
    }

    @Override
    public String toString() {
        return str;
    }

    public static PrestoType fromString(String type) {

        switch (type.toLowerCase()) {
        case StandardTypes.BIGINT:
            return BIGINT;
        case StandardTypes.BOOLEAN:
            return BOOLEAN;
        case StandardTypes.DATE:
            return DATE;
        case StandardTypes.DOUBLE:
            return DOUBLE;
        case StandardTypes.TIMESTAMP:
            return TIMESTAMP;
        case StandardTypes.TIME:
            return TIME;
        case StandardTypes.VARBINARY:
            return VARBINARY;
        case StandardTypes.VARCHAR:
            return VARCHAR;
        default:
            throw new InvalidParameterException(
                    "Unsupported accumulo type " + type);
        }
    }

    public Type spiType() {
        switch (this) {
        case BIGINT:
            return BigintType.BIGINT;
        case BOOLEAN:
            return BooleanType.BOOLEAN;
        case DATE:
            return DateType.DATE;
        case DOUBLE:
            return DoubleType.DOUBLE;
        case TIMESTAMP:
            return TimestampType.TIMESTAMP;
        case TIME:
            return TimeType.TIME;
        case VARBINARY:
            return VarbinaryType.VARBINARY;
        case VARCHAR:
            return VarcharType.VARCHAR;
        default:
            throw new InvalidParameterException(
                    "Unsupported accumulo type " + this.str);
        }
    }
}