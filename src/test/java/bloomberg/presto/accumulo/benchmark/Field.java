package bloomberg.presto.accumulo.benchmark;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import bloomberg.presto.accumulo.PrestoType;

public class Field {
    private Object value;
    private PrestoType type;

    public Field(Object v, PrestoType t) {
        this.value = v;
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
        if (obj instanceof Field) {
            Field f = (Field) obj;
            return this.getType().equals(f.getType())
                    && this.getValue().equals(f.getValue());
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
