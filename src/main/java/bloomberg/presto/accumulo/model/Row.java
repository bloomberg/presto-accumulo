package bloomberg.presto.accumulo.model;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.type.Type;

public class Row implements Comparable<Row> {
    private List<Field> fields = new ArrayList<>();

    public Row() {

    }

    public Row(Row row) {
        for (Field f : row.fields) {
            fields.add(new Field(f));
        }
    }

    public static Row newInstance() {
        return new Row();
    }

    public Row addField(Field f) {
        fields.add(f);
        return this;
    }

    public Row addField(Object v, Type t) {
        fields.add(new Field(v, t));
        return this;
    }

    public Field getField(int i) {
        return fields.get(i);
    }

    public List<Field> getFields() {
        return fields;
    }

    public int length() {
        return fields.size();
    }

    @Override
    public int compareTo(Row o) {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof Row) {
            Row r = (Row) obj;
            int i = 0;
            for (Field f : r.getFields()) {
                if (!this.fields.get(i++).equals(f)) {
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder bldr = new StringBuilder("(");
        for (Field f : fields) {
            bldr.append(f).append(",");
        }

        if (bldr.length() > 0) {
            bldr.deleteCharAt(bldr.length() - 1);
        }
        return bldr.append(')').toString();
    }
}
