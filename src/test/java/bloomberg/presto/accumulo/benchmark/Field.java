package bloomberg.presto.accumulo.benchmark;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;

import bloomberg.presto.accumulo.PrestoType;

public class Field {
    private Object value;
    private PrestoType type;

    public Field(Object v, PrestoType t) {
        if (v instanceof Time) {
            // Although the milliseconds are stored in Accumulo,
            // JDBC results return the Time object with the year/month/day set
            // to 1970-01-01
            // We truncate the Time here so our tests will be successful, as
            // Time.equals seems to compare the Dates as well as the times
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(((Time) v).getTime());
            cal.set(Calendar.YEAR, 1970);
            cal.set(Calendar.MONTH, 1);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            this.value = new Time(cal.getTimeInMillis());
        } else {
            this.value = v;
        }

        this.type = t;
    }

    public PrestoType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public String getString() {
        return value.toString();
    }

    public Long getBigInt() {
        return (Long) value;
    }

    public Boolean getBoolean() {
        return (Boolean) value;
    }

    public Date getDate() {
        return (Date) value;
    }

    public Double getDouble() {
        return (Double) value;
    }

    public Object getIntervalDatToSecond() {
        throw new UnsupportedOperationException();
    }

    public Object getIntervalYearToMonth() {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp() {
        return (Timestamp) value;
    }

    public Object getTimestampWithTimeZone() {
        throw new UnsupportedOperationException();
    }

    public Time getTime() {
        return (Time) value;
    }

    public Object getTimeWithTimeZone() {
        throw new UnsupportedOperationException();
    }

    public byte[] getVarBinary() {
        return (byte[]) value;
    }

    public String getVarChar() {
        return (String) value;
    }

    @Override
    public boolean equals(Object obj) {
        boolean retval = false;
        if (obj instanceof Field) {
            Field f = (Field) obj;
            if (type.equals(f.getType())) {
                if (type.equals(PrestoType.VARBINARY)) {
                    // special case for byte arrays
                    // aren't they so fancy
                    retval = Arrays.equals((byte[]) value,
                            (byte[]) f.getValue());
                } else {
                    retval = value.equals(f.getValue());
                }
            }
        }
        return retval;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
