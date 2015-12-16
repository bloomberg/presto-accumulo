package bloomberg.presto.accumulo;

import com.facebook.presto.spi.type.StandardTypes;

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
}