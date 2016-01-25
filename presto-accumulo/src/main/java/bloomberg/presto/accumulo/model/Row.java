package bloomberg.presto.accumulo.model;

import com.facebook.presto.spi.type.Type;
import org.apache.commons.lang.StringUtils;

import java.security.InvalidParameterException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class Row
        implements Comparable<Row>
{
    private List<Field> fields = new ArrayList<>();

    public Row()
    {}

    public Row(Row row)
    {
        for (Field f : row.fields) {
            fields.add(new Field(f));
        }
    }

    public static Row newInstance()
    {
        return new Row();
    }

    public Row addField(Field f)
    {
        fields.add(f);
        return this;
    }

    public Row addField(Object v, Type t)
    {
        fields.add(new Field(v, t));
        return this;
    }

    public Field getField(int i)
    {
        return fields.get(i);
    }

    public List<Field> getFields()
    {
        return fields;
    }

    public int length()
    {
        return fields.size();
    }

    @Override
    public int compareTo(Row o)
    {
        return 0;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(fields.toArray());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Row) {
            Row r = (Row) obj;
            int i = 0;
            for (Field f : r.getFields()) {
                if (!this.fields.get(i++).equals(f)) {
                    return false;
                }
            }

            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder bldr = new StringBuilder("(");
        for (Field f : fields) {
            bldr.append(f).append(",");
        }

        if (bldr.length() > 0) {
            bldr.deleteCharAt(bldr.length() - 1);
        }
        return bldr.append(')').toString();
    }

    public static Row fromString(RowSchema schema, String str, char delimiter)
    {
        Row r = Row.newInstance();

        String[] fields = StringUtils.split(str, delimiter);

        if (fields.length != schema.getLength()) {
            throw new InvalidParameterException("Number of split tokens is not equal to schema length");
        }

        for (int i = 0; i < fields.length; ++i) {
            Type type = schema.getColumn(i).getType();

            if (type == BIGINT) {
                r.addField(Long.parseLong(fields[i]), BIGINT);
            }
            else if (type == BOOLEAN) {
                r.addField(Boolean.parseBoolean(fields[i]), BOOLEAN);
            }
            else if (type == DATE) {
                r.addField(new Date(TimeUnit.MILLISECONDS.toDays(Date.valueOf(fields[i]).getTime())), DATE);
            }
            else if (type == DOUBLE) {
                r.addField(Double.parseDouble(fields[i]), DOUBLE);
            }
            else if (type == TIME) {
                r.addField(Time.valueOf(fields[i]), TIME);
            }
            else if (type == TIMESTAMP) {
                r.addField(Timestamp.valueOf(fields[i]), TIMESTAMP);
            }
            else if (type == VARBINARY) {
                r.addField(fields[i].getBytes(), VARBINARY);
            }
            else if (type == VARCHAR) {
                r.addField(fields[i], VARCHAR);
            }
            else {
                throw new UnsupportedOperationException("Unsupported type " + type);
            }
        }

        return r;
    }
}
