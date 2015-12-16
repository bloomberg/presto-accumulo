package bloomberg.presto.accumulo.benchmark;

import bloomberg.presto.accumulo.PrestoType;

public class Column {
    private String prestoName;
    private String columnFamily;
    private String columnQualifier;
    private PrestoType type;
    private int ordinal;

    public Column(String prestoName, String columnFamily,
            String columnQualifier, PrestoType type, int ordinal) {
        this.prestoName = prestoName;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.type = type;
        this.ordinal = ordinal;
    }

    public String getPrestoName() {
        return prestoName;
    }

    public void setPrestoName(String prestoName) {
        this.prestoName = prestoName;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getColumnQualifier() {
        return columnQualifier;
    }

    public void setColumnQualifier(String columnQualifier) {
        this.columnQualifier = columnQualifier;
    }

    public PrestoType getType() {
        return type;
    }

    public void setType(PrestoType type) {
        this.type = type;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }
}
