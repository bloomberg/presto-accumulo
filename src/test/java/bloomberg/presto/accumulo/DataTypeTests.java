package bloomberg.presto.accumulo;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.junit.After;
import org.junit.Test;

import bloomberg.presto.accumulo.benchmark.QueryDriver;
import bloomberg.presto.accumulo.storage.Row;
import bloomberg.presto.accumulo.storage.RowSchema;

public class DataTypeTests {

    public static final QueryDriver HARNESS;

    static {
        try {
            HARNESS = new QueryDriver("default", "localhost:2181", "root",
                    "secret");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void cleanup() throws Exception {
        HARNESS.cleanup();
    }

    @Test
    public void testSelectBigInt() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", PrestoType.BIGINT);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Long(28), PrestoType.BIGINT);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Long(0), PrestoType.BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectBoolean() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "male", PrestoType.BOOLEAN);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Boolean(true), PrestoType.BOOLEAN);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Boolean(false), PrestoType.BOOLEAN);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectDate() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "start_date", PrestoType.DATE);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Date(new GregorianCalendar(2015, 12, 14).getTime()
                        .getTime()), PrestoType.DATE);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Date(new GregorianCalendar(2015, 12, 15).getTime()
                        .getTime()), PrestoType.DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectDouble() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "rate", PrestoType.DOUBLE);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Double(28.1234), PrestoType.DOUBLE);

        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Double(-123.1234), PrestoType.DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectTime() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "last_login", PrestoType.TIME);

        Calendar cal = new GregorianCalendar();
        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Time(cal.getTimeInMillis()), PrestoType.TIME);

        cal.add(Calendar.MINUTE, 5);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Time(cal.getTimeInMillis()), PrestoType.TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectTimestamp() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "last_login", PrestoType.TIMESTAMP);

        Calendar cal = new GregorianCalendar();
        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Timestamp(cal.getTimeInMillis()),
                        PrestoType.TIMESTAMP);

        cal.add(Calendar.MINUTE, 5);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Timestamp(cal.getTimeInMillis()),
                        PrestoType.TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectVarbinary() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "bytes", PrestoType.VARBINARY);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField("Check out all this data!".getBytes(),
                        PrestoType.VARBINARY);

        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField("Check out all this other data!".getBytes(),
                        PrestoType.VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectVarchar() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "name", PrestoType.VARCHAR);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField("Alice", PrestoType.VARCHAR);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField("Bob", PrestoType.VARCHAR);
        Row r3 = Row.newInstance().addField("row3", PrestoType.VARCHAR)
                .addField("Carol", PrestoType.VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withInput(r3).withOutput(r1).withOutput(r2).withOutput(r3)
                .runTest();
    }
}
