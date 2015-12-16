package bloomberg.presto.accumulo;

import java.security.InvalidParameterException;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntervalDayTimeType;
import com.facebook.presto.spi.type.IntervalYearMonthType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;

/**
 * An enum wrapper for Presto's {@link StandardTypes}, because enums are much
 * easier to work with for this kind of stuff
 */
public enum PrestoType {

    BIGINT(StandardTypes.BIGINT), BOOLEAN(StandardTypes.BOOLEAN), DATE(
            StandardTypes.DATE), DOUBLE(
                    StandardTypes.DOUBLE), INTERVAL_DAY_TO_SECOND(
                            StandardTypes.INTERVAL_DAY_TO_SECOND), INTERVAL_YEAR_TO_MONTH(
                                    StandardTypes.INTERVAL_YEAR_TO_MONTH), TIME(
                                            StandardTypes.TIME), TIME_WITH_TIME_ZONE(
                                                    StandardTypes.TIME_WITH_TIME_ZONE), TIMESTAMP(
                                                            StandardTypes.TIMESTAMP), TIMESTAMP_WITH_TIME_ZONE(
                                                                    StandardTypes.TIMESTAMP_WITH_TIME_ZONE), VARBINARY(
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

        switch (type) {
        case StandardTypes.BIGINT:
            return BIGINT;
        case StandardTypes.BOOLEAN:
            return BOOLEAN;
        case StandardTypes.DATE:
            return DATE;
        case StandardTypes.DOUBLE:
            return DOUBLE;
        case StandardTypes.INTERVAL_DAY_TO_SECOND:
            return INTERVAL_DAY_TO_SECOND;
        case StandardTypes.INTERVAL_YEAR_TO_MONTH:
            return INTERVAL_YEAR_TO_MONTH;
        case StandardTypes.TIMESTAMP:
            return TIMESTAMP;
        case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
            return TIMESTAMP_WITH_TIME_ZONE;
        case StandardTypes.TIME:
            return TIME;
        case StandardTypes.TIME_WITH_TIME_ZONE:
            return TIME_WITH_TIME_ZONE;
        case StandardTypes.VARBINARY:
            return VARBINARY;
        case StandardTypes.VARCHAR:
            return VARCHAR;
        default:
            throw new InvalidParameterException("Unsupported type " + type);
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
        case INTERVAL_DAY_TO_SECOND:
            return IntervalDayTimeType.INTERVAL_DAY_TIME;
        case INTERVAL_YEAR_TO_MONTH:
            return IntervalYearMonthType.INTERVAL_YEAR_MONTH;
        case TIMESTAMP:
            return TimestampType.TIMESTAMP;
        case TIMESTAMP_WITH_TIME_ZONE:
            return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        case TIME:
            return TimeType.TIME;
        case TIME_WITH_TIME_ZONE:
            return TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
        case VARBINARY:
            return VarbinaryType.VARBINARY;
        case VARCHAR:
            return VarcharType.VARCHAR;
        default:
            throw new InvalidParameterException("Unsupported type " + this.str);
        }
    }
}