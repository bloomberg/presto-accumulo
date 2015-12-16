package bloomberg.presto.accumulo;

import java.sql.Date;
import java.util.GregorianCalendar;

import org.junit.Ignore;
import org.junit.Test;

import bloomberg.presto.accumulo.benchmark.QueryDriver;
import bloomberg.presto.accumulo.benchmark.Row;
import bloomberg.presto.accumulo.benchmark.RowSchema;

public class DataTypeTests {

    @Test
    public void testSelectBigInt() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", PrestoType.BIGINT);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Long(28), PrestoType.BIGINT);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Long(0), PrestoType.BIGINT);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectBoolean() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "male", PrestoType.BOOLEAN);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Boolean(true), PrestoType.BOOLEAN);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Boolean(false), PrestoType.BOOLEAN);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectDate() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "start_date", PrestoType.DATE);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Date(new GregorianCalendar(2015, 12, 14).getTime()
                        .getTime()), PrestoType.DATE);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Date(new GregorianCalendar(2015, 12, 15).getTime()
                        .getTime()), PrestoType.DATE);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    public void testSelectDouble() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", PrestoType.DOUBLE);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Double(28.1234), PrestoType.DOUBLE);

        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField(new Double(-123.1234), PrestoType.DOUBLE);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withOutput(r1).withOutput(r2).runTest();
    }

    @Test
    @Ignore
    public void testSelectTime() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", PrestoType.TIME);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Long(28), PrestoType.TIME);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withOutput(r1).runTest();
    }

    @Test
    @Ignore
    public void testSelectTimeWithTimeZone() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", PrestoType.TIME_WITH_TIME_ZONE);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Long(28), PrestoType.TIME_WITH_TIME_ZONE);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withOutput(r1).runTest();
    }

    @Test
    @Ignore
    public void testSelectTimestamp() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", PrestoType.TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Long(28), PrestoType.TIMESTAMP);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withOutput(r1).runTest();
    }

    @Test
    @Ignore
    public void testSelectTimestampWithTimeZone() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", PrestoType.TIMESTAMP_WITH_TIME_ZONE);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Long(28), PrestoType.TIMESTAMP_WITH_TIME_ZONE);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withOutput(r1).runTest();
    }

    @Test
    @Ignore
    public void testSelectVarbinary() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", PrestoType.VARBINARY);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField(new Long(28), PrestoType.VARBINARY);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withOutput(r1).runTest();
    }

    @Test
    public void testSelectVarchar() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "name", PrestoType.VARCHAR);

        Row r1 = Row.newInstance().addField("row1", PrestoType.VARCHAR)
                .addField("Alice", PrestoType.VARCHAR);
        Row r2 = Row.newInstance().addField("row2", PrestoType.VARCHAR)
                .addField("Bob", PrestoType.VARCHAR);
        Row r3 = Row.newInstance().addField("row3", PrestoType.VARCHAR)
                .addField("Carol", PrestoType.VARCHAR);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withInput(r3).withOutput(r1).withOutput(r2).withOutput(r3)
                .runTest();
    }
}
