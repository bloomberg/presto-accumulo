package bloomberg.presto.accumulo.integration.tests;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.junit.After;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.model.RowSchema;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class PredicatePushdownTest {

    public static final QueryDriver HARNESS;

    private static final Block ARRAY_A = AccumuloRowSerializer
            .getBlockFromArray(VARCHAR, ImmutableList.of("a"));
    private static final Block ARRAY_ABC = AccumuloRowSerializer
            .getBlockFromArray(VARCHAR, ImmutableList.of("a", "b", "c"));
    private static final Block ARRAY_DEF = AccumuloRowSerializer
            .getBlockFromArray(VARCHAR, ImmutableList.of("d", "e", "f"));
    private static final Block ARRAY_GHI = AccumuloRowSerializer
            .getBlockFromArray(VARCHAR, ImmutableList.of("g", "h", "i"));
    private static final Block ARRAY_JKL = AccumuloRowSerializer
            .getBlockFromArray(VARCHAR, ImmutableList.of("j", "k", "l"));
    private static final Block ARRAY_MNO = AccumuloRowSerializer
            .getBlockFromArray(VARCHAR, ImmutableList.of("m", "n", "o"));

    private static final Block MAP_A = AccumuloRowSerializer.getBlockFromMap(
            new MapType(VARCHAR, BIGINT), ImmutableMap.of("a", 1));
    private static final Block MAP_ABC = AccumuloRowSerializer.getBlockFromMap(
            new MapType(VARCHAR, BIGINT),
            ImmutableMap.of("a", 1, "b", 2, "c", 3));
    private static final Block MAP_DEF = AccumuloRowSerializer.getBlockFromMap(
            new MapType(VARCHAR, BIGINT),
            ImmutableMap.of("d", 4, "e", 5, "f", 6));
    private static final Block MAP_GHI = AccumuloRowSerializer.getBlockFromMap(
            new MapType(VARCHAR, BIGINT),
            ImmutableMap.of("g", 7, "h", 8, "i", 9));
    private static final Block MAP_JKL = AccumuloRowSerializer.getBlockFromMap(
            new MapType(VARCHAR, BIGINT),
            ImmutableMap.of("j", 0, "k", 1, "l", 2));
    private static final Block MAP_MNO = AccumuloRowSerializer.getBlockFromMap(
            new MapType(VARCHAR, BIGINT),
            ImmutableMap.of("m", 3, "n", 4, "o", 5));

    static {
        try {
            AccumuloConfig config = new AccumuloConfig();
            config.setInstance("default");
            config.setZooKeepers("localhost:2181");
            config.setUsername("root");
            config.setPassword("secret");
            HARNESS = new QueryDriver(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void cleanup() throws Exception {
        HARNESS.cleanup();
    }

    @Test
    public void testSelectBigIntWhereNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("age", "metadata", "age", BIGINT)
                .addColumn("weight", "metadata", "weight", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT).addField(new Long(60), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, BIGINT).addField(new Long(60), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT).addField(new Long(60), BIGINT);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectBigIntWhereNotNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("age", "metadata", "age", BIGINT)
                .addColumn("weight", "metadata", "weight", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT).addField(new Long(60), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, BIGINT).addField(new Long(60), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT).addField(new Long(60), BIGINT);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectBigIntWhereNullOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("age", "metadata", "age", BIGINT)
                .addColumn("weight", "metadata", "weight", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT).addField(new Long(60), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, BIGINT).addField(new Long(60), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT).addField(new Long(60), BIGINT);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age = 0 OR age is NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectBigIntLess() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age < 15")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectBigIntLessOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age <= 15")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectBigIntEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age = 15")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age = 15 AND age IS NOT NULL")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withInput(r3).withOutput(r2).runTest();

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age = 15 OR age IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectBigIntGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age > 15")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    @Test
    public void testSelectBigIntGreaterOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age >= 15")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectBigIntLessAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age < 30 AND age > 0")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectBigIntLessEqualAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Long(-15), BIGINT);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Long(45), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age <= 30 AND age > 0")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectBigIntLessAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Long(-15), BIGINT);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Long(45), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age < 30 AND age >= 0")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectBigIntLessEqualAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Long(-15), BIGINT);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Long(45), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age <= 30 AND age >= 0")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectBigIntInRange() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Long(-15), BIGINT);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Long(45), BIGINT);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(new Long(60), BIGINT);
        Row r7 = Row.newInstance().addField("row7", VARCHAR)
                .addField(new Long(90), BIGINT);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age IN (0, 15, -15, 45)")
                .withInputSchema(schema).withSplits("row4", "row6")
                .withInput(r1, r2, r3, r4, r5, r6, r7)
                .withOutput(r1, r2, r4, r5).runTest();
    }

    @Test
    public void testSelectBigIntWithOr() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Long(-15), BIGINT);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Long(45), BIGINT);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(new Long(60), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE (age <= 30 AND age >= 0) OR (age > 45 AND age < 70)")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r2, r3, r6).runTest();
    }

    @Test
    public void testSelectBigIntWithComplexWhereing() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("age", "metadata", "age", BIGINT)
                .addColumn("fav_num", "metadata", "fav_num", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT).addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT).addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT).addField(new Long(30), BIGINT);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Long(-15), BIGINT)
                .addField(new Long(-15), BIGINT);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Long(45), BIGINT).addField(new Long(45), BIGINT);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(new Long(60), BIGINT).addField(new Long(60), BIGINT);
        Row r7 = Row.newInstance().addField("row7", VARCHAR)
                .addField(new Long(90), BIGINT).addField(new Long(90), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE (("
                        + "age <= 30 AND age >= 0) OR ("
                        + "age > 45 AND age < 70) OR (age > 80)) AND (("
                        + "fav_num <= 30 AND fav_num > 15) OR ("
                        + "fav_num >= 45 AND fav_num < 60) OR (fav_num = 90))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6, r7)
                .withOutput(r3, r7).runTest();
    }

    @Test
    public void testSelectBooleanWhereNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("male", "metadata", "male", BOOLEAN)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(null, BOOLEAN).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(false, BOOLEAN).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE male IS NULL")
                .withInputSchema(schema).withInput(r1, r2).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectBooleanWhereNotNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("male", "metadata", "male", BOOLEAN)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(null, BOOLEAN).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(false, BOOLEAN).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE male IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectBooleanWhereNullOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("male", "metadata", "male", BOOLEAN)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(null, BOOLEAN).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(false, BOOLEAN).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(true, BOOLEAN).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE male = false OR male IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectBooleanIsTrue() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("male",
                "metadata", "male", BOOLEAN);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Boolean(true), BOOLEAN);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Boolean(false), BOOLEAN);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE male = true")
                .withInputSchema(schema).withInput(r1, r2).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectBooleanIsFalse() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("male",
                "metadata", "male", BOOLEAN);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(true,
                BOOLEAN);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(false,
                BOOLEAN);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE male = false")
                .withInputSchema(schema).withInput(r1, r2).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectBooleanIsTrueAndFalse() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("male", "metadata", "male", BOOLEAN)
                .addColumn("human", "metadata", "human", BOOLEAN);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(true, BOOLEAN).addField(true, BOOLEAN);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(false, BOOLEAN).addField(true, BOOLEAN);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(true, BOOLEAN).addField(false, BOOLEAN);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE male = false AND human = true")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectBooleanInRange() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("male",
                "metadata", "male", BOOLEAN);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(true,
                BOOLEAN);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(false,
                BOOLEAN);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE male IN (false, true)")
                .withInputSchema(schema).withInput(r1, r2).withOutput(r1, r2)
                .runTest();
    }

    @Test
    public void testSelectDateWhereNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, DATE).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE start_date IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectDateWhereNotNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, DATE).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectDateWhereNullOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, DATE).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date = DATE '2015-12-01' OR start_date IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectDateLess() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date < DATE '2015-12-15'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectDateLessOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date <= DATE '2015-12-15'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectDateEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date = DATE '2015-12-15'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectDateGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date > DATE '2015-12-15'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    @Test
    public void testSelectDateGreaterOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date >= DATE '2015-12-15'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectDateLessAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date > DATE '2015-12-01' AND "
                        + "start_date < DATE '2015-12-31'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectDateLessEqualAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(c(2015, 11, 15), DATE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(c(2016, 1, 15), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date > DATE '2015-12-01' AND "
                        + "start_date <= DATE '2015-12-31'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectDateLessAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(c(2015, 11, 15), DATE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(c(2016, 1, 15), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date >= DATE '2015-12-01' AND "
                        + "start_date < DATE '2015-12-31'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectDateLessEqualAndGreaterEqual() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(c(2015, 11, 15), DATE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(c(2016, 1, 15), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date >= DATE '2015-12-01' AND "
                        + "start_date <= DATE '2015-12-31'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectDateWithOr() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(c(2015, 11, 15), DATE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(c(2016, 1, 15), DATE);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(c(2016, 1, 31), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ("
                        + "(start_date > DATE '2015-12-01' AND "
                        + "start_date <= DATE '2015-12-31')) OR ("
                        + "(start_date >= DATE '2016-01-15' AND "
                        + "start_date < DATE '2016-01-31'))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r2, r3, r5).runTest();
    }

    @Test
    public void testSelectDateInRange() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(c(2015, 12, 1), DATE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(c(2015, 12, 15), DATE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(c(2015, 12, 31), DATE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(c(2015, 11, 15), DATE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(c(2016, 1, 15), DATE);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(c(2016, 1, 31), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE start_date IN ("
                        + "DATE '2015-12-01', " + "DATE '2015-12-31', "
                        + "DATE '2016-01-15', " + "DATE '2015-11-15')")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r3, r5, r4).runTest();
    }

    @Test
    public void testSelectDoubleWhereNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("age", "metadata", "age", DOUBLE)
                .addColumn("weight", "metadata", "weight", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE)
                .addField(new Double(60), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, DOUBLE).addField(new Double(60), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE)
                .addField(new Double(60), DOUBLE);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectDoubleWhereNotNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("age", "metadata", "age", DOUBLE)
                .addColumn("weight", "metadata", "weight", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE)
                .addField(new Double(60), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, DOUBLE).addField(new Double(60), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE)
                .addField(new Double(60), DOUBLE);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectDoubleWhereNullOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("age", "metadata", "age", DOUBLE)
                .addColumn("weight", "metadata", "weight", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE)
                .addField(new Double(60), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, DOUBLE).addField(new Double(60), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE)
                .addField(new Double(60), DOUBLE);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age = 0.0 OR age is NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectDoubleLess() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age < 15")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectDoubleLessOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age <= 15")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectDoubleEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age = 15")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age = 15 AND age IS NOT NULL")
                .withInputSchema(schema).withInput(r1).withInput(r2)
                .withInput(r3).withOutput(r2).runTest();

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age = 15 OR age IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectDoubleGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age > 15")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    @Test
    public void testSelectDoubleGreaterOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age >= 15")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectDoubleLessAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age < 30 AND age > 0")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectDoubleLessEqualAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Double(-15), DOUBLE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Double(45), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age <= 30 AND age > 0")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectDoubleLessAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Double(-15), DOUBLE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Double(45), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age < 30 AND age >= 0")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectDoubleLessEqualAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Double(-15), DOUBLE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Double(45), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age <= 30 AND age >= 0")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectDoubleInRange() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Double(-15), DOUBLE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Double(45), DOUBLE);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(new Double(60), DOUBLE);
        Row r7 = Row.newInstance().addField("row7", VARCHAR)
                .addField(new Double(90), DOUBLE);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE age IN (0, 15, -15, 45)")
                .withInputSchema(schema).withSplits("row4", "row6")
                .withInput(r1, r2, r3, r4, r5, r6, r7)
                .withOutput(r1, r2, r4, r5).runTest();
    }

    @Test
    public void testSelectDoubleWithOr() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Double(-15), DOUBLE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Double(45), DOUBLE);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(new Double(60), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE (age <= 30 AND age >= 0) OR (age > 45 AND age < 70)")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r2, r3, r6).runTest();
    }

    @Test
    public void testSelectDoubleWithComplexWhereing() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("age", "metadata", "age", DOUBLE)
                .addColumn("fav_num", "metadata", "fav_num", DOUBLE);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Double(0), DOUBLE)
                .addField(new Double(0), DOUBLE);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Double(15), DOUBLE)
                .addField(new Double(15), DOUBLE);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Double(30), DOUBLE)
                .addField(new Double(30), DOUBLE);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Double(-15), DOUBLE)
                .addField(new Double(-15), DOUBLE);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Double(45), DOUBLE)
                .addField(new Double(45), DOUBLE);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(new Double(60), DOUBLE)
                .addField(new Double(60), DOUBLE);
        Row r7 = Row.newInstance().addField("row7", VARCHAR)
                .addField(new Double(90), DOUBLE)
                .addField(new Double(90), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE (("
                        + "age <= 30 AND age >= 0) OR ("
                        + "age > 45 AND age < 70) OR (age > 80)) AND (("
                        + "fav_num <= 30 AND fav_num > 15) OR ("
                        + "fav_num >= 45 AND fav_num < 60) OR (fav_num = 90))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6, r7)
                .withOutput(r3, r7).runTest();
    }

    @Test
    public void testSelectTimeWhereNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, TIME).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE start_date IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectTimeWhereNotNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, TIME).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectTimeWhereNullOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, TIME).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date = TIME '00:30:00' OR start_date IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectTimeLess() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date < TIME '12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectTimeLessOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date <= TIME '12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectTimeEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date = TIME '12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectTimeGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date > TIME '12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    @Test
    public void testSelectTimeGreaterOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date >= TIME '12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectTimeLessAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date > TIME '00:30:00' AND "
                        + "start_date < TIME '20:15:30'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectTimeLessEqualAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(t(0, 0, 0), TIME);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(t(22, 0, 15), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date > TIME '00:30:00' AND "
                        + "start_date <= TIME '20:15:30'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectTimeLessAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(t(0, 0, 0), TIME);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(t(22, 0, 15), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date >= TIME '00:30:00' AND "
                        + "start_date < TIME '20:15:30'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectTimeLessEqualAndGreaterEqual() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(t(0, 0, 0), TIME);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(t(22, 0, 15), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date >= TIME '00:30:00' AND "
                        + "start_date <= TIME '20:15:30'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectTimeWithOr() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(t(0, 0, 0), TIME);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(t(22, 0, 15), TIME);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(ts(2016, 01, 31, 23, 0, 0), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ("
                        + "(start_date > TIME '00:30:00' AND "
                        + "start_date <= TIME '20:15:30')) OR ("
                        + "(start_date >= TIME '22:00:15' AND "
                        + "start_date < TIME '23:00:00'))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r2, r3, r5).runTest();
    }

    @Test
    public void testSelectTimeInRange() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIME);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(t(0, 30, 0), TIME);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(t(12, 0, 30), TIME);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(t(20, 15, 30), TIME);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(t(0, 0, 0), TIME);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(t(22, 0, 15), TIME);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(ts(2016, 01, 31, 23, 0, 0), TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE start_date IN ("
                        + "TIME '00:30:00', " + "TIME '20:15:30', "
                        + "TIME '22:00:15', " + "TIME '00:00:00')")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r3, r5, r4).runTest();
    }

    @Test
    public void testSelectTimestampWhereNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP)
                .addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, TIMESTAMP).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP)
                .addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE start_date IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectTimestampWhereNotNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP)
                .addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, TIMESTAMP).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP)
                .addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectTimestampWhereNullOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP)
                .addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, TIMESTAMP).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP)
                .addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date = TIMESTAMP '2015-12-01 00:30:00' OR start_date IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectTimestampLess() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date < TIMESTAMP '2015-12-15 12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectTimestampLessOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date <= TIMESTAMP '2015-12-15 12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectTimestampEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date = TIMESTAMP '2015-12-15 12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectTimestampGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date > TIMESTAMP '2015-12-15 12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    @Test
    public void testSelectTimestampGreaterOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE start_date >= TIMESTAMP '2015-12-15 12:00:30'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectTimestampLessAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date > TIMESTAMP '2015-12-01 00:30:00' AND "
                        + "start_date < TIMESTAMP '2015-12-31 20:15:30'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectTimestampLessEqualAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(ts(2015, 11, 15, 0, 0, 0), TIMESTAMP);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(ts(2016, 01, 15, 22, 0, 15), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date > TIMESTAMP '2015-12-01 00:30:00' AND "
                        + "start_date <= TIMESTAMP '2015-12-31 20:15:30'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectTimestampLessAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(ts(2015, 11, 15, 0, 0, 0), TIMESTAMP);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(ts(2016, 01, 15, 22, 0, 15), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date >= TIMESTAMP '2015-12-01 00:30:00' AND "
                        + "start_date < TIMESTAMP '2015-12-31 20:15:30'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectTimestampLessEqualAndGreaterEqual() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(ts(2015, 11, 15, 0, 0, 0), TIMESTAMP);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(ts(2016, 01, 15, 22, 0, 15), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "start_date >= TIMESTAMP '2015-12-01 00:30:00' AND "
                        + "start_date <= TIMESTAMP '2015-12-31 20:15:30'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectTimestampWithOr() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(ts(2015, 11, 15, 0, 0, 0), TIMESTAMP);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(ts(2016, 01, 15, 22, 0, 15), TIMESTAMP);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(ts(2016, 01, 31, 23, 0, 0), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ("
                        + "(start_date > TIMESTAMP '2015-12-01 00:30:00' AND "
                        + "start_date <= TIMESTAMP '2015-12-31 20:15:30')) OR ("
                        + "(start_date >= TIMESTAMP '2016-01-15 22:00:15' AND "
                        + "start_date < TIMESTAMP '2016-01-31 23:00:00'))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r2, r3, r5).runTest();
    }

    @Test
    public void testSelectTimestampInRange() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("start_date", "metadata", "start_date", TIMESTAMP);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ts(2015, 12, 1, 0, 30, 0), TIMESTAMP);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(ts(2015, 12, 15, 12, 0, 30), TIMESTAMP);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ts(2015, 12, 31, 20, 15, 30), TIMESTAMP);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(ts(2015, 11, 15, 0, 0, 0), TIMESTAMP);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(ts(2016, 01, 15, 22, 0, 15), TIMESTAMP);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(ts(2016, 01, 31, 23, 0, 0), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE start_date IN ("
                        + "TIMESTAMP '2015-12-01 00:30:00', "
                        + "TIMESTAMP '2015-12-31 20:15:30', "
                        + "TIMESTAMP '2016-01-15 22:00:15', "
                        + "TIMESTAMP '2015-11-15 00:00:00')")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r3, r5, r4).runTest();
    }

    @Test
    public void testSelectVarbinaryWhereNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("bytes", "metadata", "bytes", VARBINARY)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, VARBINARY).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE bytes IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectVarbinaryWhereNotNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("bytes", "metadata", "bytes", VARBINARY)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, VARBINARY).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE bytes IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectVarbinaryWhereNullOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("bytes", "metadata", "bytes", VARBINARY)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, VARBINARY).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE bytes = VARBINARY 'abc' OR bytes IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVarbinaryLess() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE bytes < VARBINARY 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectVarbinaryLessOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE bytes <= VARBINARY 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVarbinaryEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE bytes = VARBINARY 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectVarbinaryGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE bytes > VARBINARY 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    @Test
    public void testSelectVarbinaryGreaterOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE bytes >= VARBINARY 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectVarbinaryLessAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "bytes > VARBINARY 'abc' AND "
                        + "bytes < VARBINARY 'ghi'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectVarbinaryLessEqualAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField("a".getBytes(), VARBINARY);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField("jkl".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "bytes > VARBINARY 'abc' AND "
                        + "bytes <= VARBINARY 'ghi'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectVarbinaryLessAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField("a".getBytes(), VARBINARY);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField("jkl".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "bytes >= VARBINARY 'abc' AND "
                        + "bytes < VARBINARY 'ghi'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVarbinaryLessEqualAndGreaterEqual() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField("a".getBytes(), VARBINARY);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField("jkl".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "bytes >= VARBINARY 'abc' AND "
                        + "bytes <= VARBINARY 'ghi'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectVarbinaryWithOr() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField("a".getBytes(), VARBINARY);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField("jkl".getBytes(), VARBINARY);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField("mno".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ("
                        + "(bytes > VARBINARY 'abc' AND "
                        + "bytes <= VARBINARY 'ghi')) OR ("
                        + "(bytes >= VARBINARY 'jkl' AND "
                        + "bytes < VARBINARY 'mno'))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r2, r3, r5).runTest();
    }

    @Test
    public void testSelectVarbinaryInRange() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("bytes",
                "metadata", "bytes", VARBINARY);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc".getBytes(), VARBINARY);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField("def".getBytes(), VARBINARY);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi".getBytes(), VARBINARY);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField("a".getBytes(), VARBINARY);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField("jkl".getBytes(), VARBINARY);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField("mno".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE bytes IN ("
                        + "VARBINARY 'abc', VARBINARY 'ghi', "
                        + "VARBINARY 'jkl', VARBINARY 'a')")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r3, r5, r4).runTest();
    }

    @Test
    public void testSelectVarcharWhereNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("data", "metadata", "data", VARCHAR)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc", VARCHAR).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, VARCHAR).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi", VARCHAR).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE data IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectVarcharWhereNotNull() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("data", "metadata", "data", VARCHAR)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc", VARCHAR).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, VARCHAR).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi", VARCHAR).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE data IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectVarcharWhereNullOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("data", "metadata", "data", VARCHAR)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField("abc", VARCHAR).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, VARCHAR).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField("ghi", VARCHAR).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE data = 'abc' OR data IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVarcharLess() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE data < 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectVarcharLessOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE data <= 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVarcharEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE data = 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectVarcharGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE data > 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    @Test
    public void testSelectVarcharGreaterOrEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE data >= 'def'")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectVarcharLessAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "data > 'abc' AND " + "data < 'ghi'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectVarcharLessEqualAndGreater() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField("a",
                VARCHAR);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField("jkl",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "data > 'abc' AND " + "data <= 'ghi'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectVarcharLessAndGreaterEqual() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField("a",
                VARCHAR);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField("jkl",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "data >= 'abc' AND " + "data < 'ghi'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVarcharLessEqualAndGreaterEqual() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField("a",
                VARCHAR);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField("jkl",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "data >= 'abc' AND " + "data <= 'ghi'")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectVarcharWithOr() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField("a",
                VARCHAR);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField("jkl",
                VARCHAR);
        Row r6 = Row.newInstance().addField("row6", VARCHAR).addField("mno",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ("
                        + "(data > 'abc' AND " + "data <= 'ghi')) OR ("
                        + "(data >= 'jkl' AND " + "data < 'mno'))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r2, r3, r5).runTest();
    }

    @Test
    public void testSelectVarcharInRange() throws Exception {

        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("data",
                "metadata", "data", VARCHAR);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField("abc",
                VARCHAR);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField("def",
                VARCHAR);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField("ghi",
                VARCHAR);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField("a",
                VARCHAR);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField("jkl",
                VARCHAR);
        Row r6 = Row.newInstance().addField("row6", VARCHAR).addField("mno",
                VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE data IN ('abc', 'ghi', 'jkl', 'a')")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r3, r5, r4).runTest();
    }

    @Test
    public void testSelectArrayWhereNull() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ARRAY_ABC, arrayType).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, arrayType).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ARRAY_GHI, arrayType).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE senders IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectArrayWhereNotNull() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ARRAY_ABC, arrayType).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, arrayType).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ARRAY_GHI, arrayType).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE senders IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectArrayWhereNullOrEqual() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(ARRAY_ABC, arrayType).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, arrayType).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(ARRAY_GHI, arrayType).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE senders = ARRAY['a','b','c'] OR senders IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectArrayLess() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE senders < ARRAY['d','e','f']")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectArrayLessOrEqual() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE senders <= ARRAY['d','e','f']")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectArrayEqual() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE senders = ARRAY['d','e','f']")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectArrayGreater() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE senders > ARRAY['d','e','f']")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    @Test
    public void testSelectArrayGreaterOrEqual() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE senders >= ARRAY['d','e','f']")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectArrayLessAndGreater() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "senders > ARRAY['a','b','c'] AND "
                        + "senders < ARRAY['g','h','i']")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectArrayLessEqualAndGreater() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(ARRAY_A,
                arrayType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(ARRAY_JKL,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "senders > ARRAY['a','b','c'] AND "
                        + "senders <= ARRAY['g','h','i']")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    @Test
    public void testSelectArrayLessAndGreaterEqual() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(ARRAY_A,
                arrayType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(ARRAY_JKL,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "senders >= ARRAY['a','b','c'] AND "
                        + "senders < ARRAY['g','h','i']")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    // Operator BETWEEN(array<varchar>, array<varchar>, array<varchar>) not
    // registered
    @Test(expected = SQLException.class)
    public void testSelectArrayLessEqualAndGreaterEqual() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(ARRAY_A,
                arrayType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(ARRAY_JKL,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "senders >= ARRAY['a','b','c'] AND "
                        + "senders <= ARRAY['g','h','i']")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectArrayWithOr() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(ARRAY_A,
                arrayType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(ARRAY_JKL,
                arrayType);
        Row r6 = Row.newInstance().addField("row6", VARCHAR).addField(ARRAY_MNO,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ("
                        + "(senders > ARRAY['a','b','c'] AND "
                        + "senders <= ARRAY['g','h','i'])) OR ("
                        + "(senders >= ARRAY['j','k','l'] AND "
                        + "senders < ARRAY['m','n','o']))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r2, r3, r5).runTest();
    }

    @Test
    public void testSelectArrayInRange() throws Exception {
        ArrayType arrayType = new ArrayType(VARCHAR);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(ARRAY_ABC,
                arrayType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(ARRAY_DEF,
                arrayType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(ARRAY_GHI,
                arrayType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(ARRAY_A,
                arrayType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(ARRAY_JKL,
                arrayType);
        Row r6 = Row.newInstance().addField("row6", VARCHAR).addField(ARRAY_MNO,
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE senders IN (ARRAY['a','b','c'], ARRAY['g','h','i'], ARRAY['j','k','l'], ARRAY['a'])")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r3, r5, r4).runTest();
    }

    @Test
    public void testSelectMapWhereNull() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("ages", "metadata", "ages", mapType)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(MAP_ABC, mapType).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, mapType).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(MAP_GHI, mapType).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ages IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectMapWhereNotNull() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("ages", "metadata", "ages", mapType)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(MAP_ABC, mapType).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, mapType).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(MAP_GHI, mapType).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ages IS NOT NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r3).runTest();
    }

    @Test
    public void testSelectMapWhereNullOrEqual() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId()
                .addColumn("ages", "metadata", "ages", mapType)
                .addColumn("age", "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(MAP_ABC, mapType).addField(10L, BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(null, mapType).addField(10L, BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(MAP_GHI, mapType).addField(10L, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE ages = MAP(ARRAY['a','b','c'],ARRAY[1,2,3]) OR ages IS NULL")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    // Operator LESS_THAN(map<varchar,bigint>, map<varchar,bigint>) not
    // registered
    @Test(expected = SQLException.class)
    public void testSelectMapLess() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE ages < MAP(ARRAY['d','e','f'],ARRAY[4,5,6])")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r1)
                .runTest();
    }

    // Operator LESS_THAN_OR_EQUAL(map<varchar,bigint>, map<varchar,bigint>) not
    // registered
    @Test(expected = SQLException.class)
    public void testSelectMapLessOrEqual() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE ages <= MAP(ARRAY['d','e','f'],ARRAY[4,5,6])")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectMapEqual() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE ages = MAP(ARRAY['d','e','f'],ARRAY[4,5,6])")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    // Operator GREATER_THAN(map<varchar,bigint>, map<varchar,bigint>) not
    // registered
    @Test(expected = SQLException.class)
    public void testSelectMapGreater() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE ages > MAP(ARRAY['d','e','f'],ARRAY[4,5,6])")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r3)
                .runTest();
    }

    // Operator GREATER_THAN_OR_EQUAL(map<varchar,bigint>, map<varchar,bigint>)
    // not registered
    @Test(expected = SQLException.class)
    public void testSelectMapGreaterOrEqual() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE ages >= MAP(ARRAY['d','e','f'],ARRAY[4,5,6])")
                .withInputSchema(schema).withInput(r1, r2, r3)
                .withOutput(r2, r3).runTest();
    }

    // Operator GREATER_THAN(map<varchar,bigint>, map<varchar,bigint>) not
    // registered
    @Test(expected = SQLException.class)
    public void testSelectMapLessAndGreater() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "ages > MAP(ARRAY['a','b','c'],ARRAY[1,2,3]) AND "
                        + "ages < MAP(ARRAY['g','h','i'],ARRAY[7,8,9])")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    // Operator GREATER_THAN(map<varchar,bigint>, map<varchar,bigint>) not
    // registered
    @Test(expected = SQLException.class)
    public void testSelectMapLessEqualAndGreater() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(MAP_A,
                mapType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(MAP_JKL,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "ages > MAP(ARRAY['a','b','c'],ARRAY[1,2,3]) AND "
                        + "ages <= MAP(ARRAY['g','h','i'],ARRAY[7,8,9])")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r2, r3).runTest();
    }

    // Operator GREATER_THAN_OR_EQUAL(map<varchar,bigint>, map<varchar,bigint>)
    // not registered
    @Test(expected = SQLException.class)
    public void testSelectMapLessAndGreaterEqual() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(MAP_A,
                mapType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(MAP_JKL,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "ages >= MAP(ARRAY['a','b','c'],ARRAY[1,2,3]) AND "
                        + "ages < MAP(ARRAY['g','h','i'],ARRAY[7,8,9])")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2).runTest();
    }

    // Operator GREATER_THAN_OR_EQUAL(map<varchar,bigint>, map<varchar,bigint>)
    // not registered
    @Test(expected = SQLException.class)
    public void testSelectMapLessEqualAndGreaterEqual() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(MAP_A,
                mapType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(MAP_JKL,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE "
                        + "ages >= MAP(ARRAY['a','b','c'],ARRAY[1,2,3]) AND "
                        + "ages <= MAP(ARRAY['g','h','i'],ARRAY[7,8,9])")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5)
                .withOutput(r1, r2, r3).runTest();
    }

    // Operator GREATER_THAN(map<varchar,bigint>, map<varchar,bigint>) not
    // registered
    @Test(expected = SQLException.class)
    public void testSelectMapWithOr() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(MAP_A,
                mapType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(MAP_JKL,
                mapType);
        Row r6 = Row.newInstance().addField("row6", VARCHAR).addField(MAP_MNO,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ("
                        + "(ages > MAP(ARRAY['a','b','c'],ARRAY[1,2,3]) AND "
                        + "ages <= MAP(ARRAY['g','h','i'],ARRAY[7,8,9]))) OR ("
                        + "(ages >= MAP(ARRAY['j','k','l'],ARRAY[0,1,2]) AND "
                        + "ages < MAP(ARRAY['m','n','o'],ARRAY[3,4,5])))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r2, r3, r5).runTest();
    }

    // Query parser explodes on this query
    // > error calculating hash code for InterleavedBlock{columns=2,
    // positionCount=3}
    // > Out of range: -3074044911
    @Test(expected = SQLException.class)
    public void testSelectMapInRange() throws Exception {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("ages",
                "metadata", "ages", mapType);

        Row r1 = Row.newInstance().addField("row1", VARCHAR).addField(MAP_ABC,
                mapType);
        Row r2 = Row.newInstance().addField("row2", VARCHAR).addField(MAP_DEF,
                mapType);
        Row r3 = Row.newInstance().addField("row3", VARCHAR).addField(MAP_GHI,
                mapType);
        Row r4 = Row.newInstance().addField("row4", VARCHAR).addField(MAP_A,
                mapType);
        Row r5 = Row.newInstance().addField("row5", VARCHAR).addField(MAP_JKL,
                mapType);
        Row r6 = Row.newInstance().addField("row6", VARCHAR).addField(MAP_MNO,
                mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE ages IN ("
                        + "MAP(ARRAY['a','b','c'],ARRAY[1,2,3]), "
                        + "MAP(ARRAY['g','h','i'],ARRAY[4,5,6]), "
                        + "MAP(ARRAY['j','k','l'],ARRAY[7,8,9]), "
                        + "MAP(ARRAY['a'],ARRAY[0]))")
                .withInputSchema(schema).withInput(r1, r2, r3, r4, r5, r6)
                .withOutput(r1, r3, r5, r4).runTest();
    }

    @Test
    public void testSelectBigIntNoWhereWithSplits() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Long(-15), BIGINT);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Long(45), BIGINT);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(new Long(60), BIGINT);
        Row r7 = Row.newInstance().addField("row7", VARCHAR)
                .addField(new Long(90), BIGINT);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withQuery("SELECT * FROM testmytable")
                .withInputSchema(schema).withSplits("row4", "row6")
                .withInput(r1, r2, r3, r4, r5, r6, r7)
                .withOutput(r1, r2, r3, r4, r5, r6, r7).runTest();
    }

    @Test
    public void testSelectBigIntWhereRecordKeyIs() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE recordkey = 'row2'")
                .withInputSchema(schema).withInput(r1, r2, r3).withOutput(r2)
                .runTest();
    }

    @Test
    public void testSelectBigIntWhereRecordKeyInRange() throws Exception {
        RowSchema schema = RowSchema.newInstance().addRowId().addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newInstance().addField("row1", VARCHAR)
                .addField(new Long(0), BIGINT);
        Row r2 = Row.newInstance().addField("row2", VARCHAR)
                .addField(new Long(15), BIGINT);
        Row r3 = Row.newInstance().addField("row3", VARCHAR)
                .addField(new Long(30), BIGINT);
        Row r4 = Row.newInstance().addField("row4", VARCHAR)
                .addField(new Long(-15), BIGINT);
        Row r5 = Row.newInstance().addField("row5", VARCHAR)
                .addField(new Long(45), BIGINT);
        Row r6 = Row.newInstance().addField("row6", VARCHAR)
                .addField(new Long(60), BIGINT);
        Row r7 = Row.newInstance().addField("row7", VARCHAR)
                .addField(new Long(90), BIGINT);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery(
                        "SELECT * FROM testmytable WHERE (recordkey < 'row6' AND recordkey >= 'row3') OR recordkey = 'row1'")
                .withInputSchema(schema).withSplits("row4", "row6")
                .withInput(r1, r2, r3, r4, r5, r6, r7)
                .withOutput(r1, r3, r4, r5).runTest();
    }

    private static Calendar c(int y, int m, int d) {
        return new GregorianCalendar(y, m - 1, d);
    }

    private static Long t(int h, int m, int s) {
        return new GregorianCalendar(1970, 0, 1, h, m, s).getTime().getTime();
    }

    private static Long ts(int y, int mo, int d, int h, int mi, int s) {
        return new GregorianCalendar(y, mo - 1, d, h, mi, s).getTime()
                .getTime();
    }
}
