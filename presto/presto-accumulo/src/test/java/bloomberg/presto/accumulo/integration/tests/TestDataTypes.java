/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bloomberg.presto.accumulo.integration.tests;

import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.model.RowSchema;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestDataTypes
{
    public static final QueryDriver HARNESS;

    static {
        try {
            AccumuloConfig config = new AccumuloConfig();
            config.setInstance("default");
            config.setZooKeepers("localhost:2181");
            config.setUsername("root");
            config.setPassword("secret");
            HARNESS = new QueryDriver(config);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        HARNESS.cleanup();
    }

    @Test
    public void testSelectArray()
            throws Exception
    {
        Type elementType = VARCHAR;
        ArrayType arrayType = new ArrayType(elementType);
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(AccumuloRowSerializer
                .getBlockFromArray(elementType, ImmutableList.of("a", "b", "c")), arrayType);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(AccumuloRowSerializer
                .getBlockFromArray(elementType, ImmutableList.of("d", "e", "f")), arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectNestedArray()
            throws Exception
    {
        ArrayType nestedArrayType = new ArrayType(VARCHAR);
        ArrayType arrayType = new ArrayType(nestedArrayType);
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("senders", "metadata", "senders", arrayType);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(
                AccumuloRowSerializer.getBlockFromArray(nestedArrayType,
                        ImmutableList.of(ImmutableList.of("a", "b", "c"),
                                ImmutableList.of("d", "e", "f"), ImmutableList.of("g", "h", "i"))),
                arrayType);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(
                AccumuloRowSerializer.getBlockFromArray(nestedArrayType,
                        ImmutableList.of(ImmutableList.of("j", "k", "l"),
                                ImmutableList.of("m", "n", "o"), ImmutableList.of("p", "q", "r"))),
                arrayType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVeryNestedArray()
            throws Exception
    {
        ArrayType veryNestedArrayType = new ArrayType(VARCHAR);
        ArrayType nestedArrayType = new ArrayType(veryNestedArrayType);
        ArrayType arrayType = new ArrayType(nestedArrayType);

        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("senders", "metadata", "senders", arrayType);

        // @formatter:off
        Row r1 = Row.newRow().addField("row1", VARCHAR)
                .addField(AccumuloRowSerializer.getBlockFromArray(nestedArrayType,
                        ImmutableList.of(ImmutableList.of(ImmutableList.of("a", "b", "c"),
                                ImmutableList.of("d", "e", "f"), ImmutableList.of("g", "h", "i")),
                        ImmutableList.of(ImmutableList.of("j", "k", "l"),
                                ImmutableList.of("m", "n", "o"), ImmutableList.of("p", "q", "r")),
                        ImmutableList.of(ImmutableList.of("s", "t", "u"),
                                ImmutableList.of("v", "w", "x"),
                                ImmutableList.of("y", "z", "aa")))),
                        arrayType);
        // @formatter:on

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1)
                .withOutput(r1).runTest();
    }

    @Test
    public void testSelectUberNestedArray()
            throws Exception
    {
        ArrayType uberNestedArrayType = new ArrayType(VARCHAR);
        ArrayType veryNestedArrayType = new ArrayType(uberNestedArrayType);
        ArrayType nestedArrayType = new ArrayType(veryNestedArrayType);
        ArrayType arrayType = new ArrayType(nestedArrayType);

        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("senders", "metadata", "senders", arrayType);

        // @formatter:off
        Row r1 = Row.newRow().addField("row1", VARCHAR)
                .addField(
                        AccumuloRowSerializer
                                .getBlockFromArray(nestedArrayType,
                                        ImmutableList.of(
                                                ImmutableList.of(
                                                        ImmutableList
                                                                .of(ImmutableList.of("a", "b", "c"),
                                                                        ImmutableList.of("d", "e",
                                                                                "f"),
                                                ImmutableList.of("g", "h", "i")),
                                ImmutableList.of(ImmutableList.of("j", "k", "l"),
                                        ImmutableList.of("m", "n", "o"),
                                        ImmutableList.of("p", "q", "r")),
                        ImmutableList.of(ImmutableList.of("s", "t", "u"),
                                ImmutableList.of("v", "w", "x"), ImmutableList.of("y", "z", "aa"))),
                        ImmutableList.of(ImmutableList.of(ImmutableList.of("a", "b", "c"),
                                ImmutableList.of("d", "e", "f"), ImmutableList.of("g", "h", "i")),
                                ImmutableList.of(ImmutableList.of("j", "k", "l"),
                                        ImmutableList.of("m", "n", "o"),
                                        ImmutableList.of("p", "q", "r")),
                                ImmutableList.of(ImmutableList.of("s", "t", "u"),
                                        ImmutableList.of("v", "w", "x"),
                                        ImmutableList.of("y", "z", "aa"))),
                        ImmutableList.of(ImmutableList.of(ImmutableList.of("a", "b", "c"),
                                ImmutableList.of("d", "e", "f"), ImmutableList.of("g", "h", "i")),
                                ImmutableList.of(ImmutableList.of("j", "k", "l"),
                                        ImmutableList.of("m", "n", "o"),
                                        ImmutableList.of("p", "q", "r")),
                                ImmutableList.of(ImmutableList.of("s", "t", "u"),
                                        ImmutableList.of("v", "w", "x"),
                                        ImmutableList.of("y", "z", "aa"))))),
                        arrayType);
        // @formatter:on

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1)
                .withOutput(r1).runTest();
    }

    @Test
    public void testSelectBigInt()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR).addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(Long.valueOf(28), BIGINT);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(Long.valueOf(0), BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectBoolean()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR).addColumn("male",
                "metadata", "male", BOOLEAN);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(true, BOOLEAN);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(false, BOOLEAN);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectDate()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("start_date", "metadata", "start_date", DATE);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(c(2015, 12, 14), DATE);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(c(2015, 12, 15), DATE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectDouble()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR).addColumn("rate",
                "metadata", "rate", DOUBLE);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(Double.valueOf(28.1234), DOUBLE);

        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(Double.valueOf(-123.1234), DOUBLE);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectMap()
            throws Exception
    {
        Type keyType = VARCHAR;
        Type valueType = BIGINT;
        MapType mapType = new MapType(keyType, valueType);
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("peopleages", "metadata", "peopleages", mapType);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(AccumuloRowSerializer
                .getBlockFromMap(mapType, ImmutableMap.of("a", 1, "b", 2, "c", 3)), mapType);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(AccumuloRowSerializer
                .getBlockFromMap(mapType, ImmutableMap.of("d", 4, "e", 5, "f", 6)), mapType);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testSelectMapOfArrays()
            throws Exception
    {
        Type elementType = BIGINT;
        Type keyMapType = new ArrayType(elementType);
        Type valueMapType = new ArrayType(elementType);
        MapType mapType = new MapType(keyMapType, valueMapType);
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR).addColumn("foo",
                "metadata", "foo", mapType);

        // @formatter:off
        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(
                AccumuloRowSerializer.getBlockFromMap(mapType,
                        ImmutableMap.of(ImmutableList.of(1, 2, 3), ImmutableList.of(1, 2, 3))),
                mapType);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(
                AccumuloRowSerializer.getBlockFromMap(mapType,
                        ImmutableMap.of(ImmutableList.of(4, 5, 6), ImmutableList.of(4, 5, 6))),
                mapType);
        // @formatter:on

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testSelectMapOfMaps()
            throws Exception
    {
        Type keyType = VARCHAR;
        Type valueType = BIGINT;
        Type keyMapType = new MapType(keyType, valueType);
        Type valueMapType = new MapType(keyType, valueType);
        MapType mapType = new MapType(keyMapType, valueMapType);
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR).addColumn("foo",
                "metadata", "foo", mapType);

        // @formatter:off
        Row r1 = Row.newRow().addField("row1", VARCHAR)
                .addField(AccumuloRowSerializer.getBlockFromMap(mapType,
                        ImmutableMap.of(ImmutableMap.of("a", 1, "b", 2, "c", 3),
                                ImmutableMap.of("a", 1, "b", 2, "c", 3))),
                        mapType);
        Row r2 = Row.newRow().addField("row2", VARCHAR)
                .addField(AccumuloRowSerializer.getBlockFromMap(mapType,
                        ImmutableMap.of(ImmutableMap.of("d", 4, "e", 5, "f", 6),
                                ImmutableMap.of("d", 4, "e", 5, "f", 6))),
                        mapType);
        // @formatter:on

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectTime()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("last_login", "metadata", "last_login", TIME);

        Calendar cal = new GregorianCalendar();
        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(new Time(cal.getTimeInMillis()),
                TIME);

        cal.add(Calendar.MINUTE, 5);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField(new Time(cal.getTimeInMillis()),
                TIME);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectTimestamp()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("last_login", "metadata", "last_login", TIMESTAMP);

        Calendar cal = new GregorianCalendar();
        Row r1 = Row.newRow().addField("row1", VARCHAR)
                .addField(new Timestamp(cal.getTimeInMillis()), TIMESTAMP);

        cal.add(Calendar.MINUTE, 5);
        Row r2 = Row.newRow().addField("row2", VARCHAR)
                .addField(new Timestamp(cal.getTimeInMillis()), TIMESTAMP);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVarbinary()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("bytes", "metadata", "bytes", VARBINARY);

        Row r1 = Row.newRow().addField("row1", VARCHAR)
                .addField("Check out all this data!".getBytes(), VARBINARY);

        Row r2 = Row.newRow().addField("row2", VARCHAR)
                .addField("Check out all this other data!".getBytes(), VARBINARY);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .withOutput(r1, r2).runTest();
    }

    @Test
    public void testSelectVarchar()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR).addColumn("name",
                "metadata", "name", VARCHAR);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField("Alice", VARCHAR);
        Row r2 = Row.newRow().addField("row2", VARCHAR).addField("Bob", VARCHAR);
        Row r3 = Row.newRow().addField("row3", VARCHAR).addField("Carol", VARCHAR);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema)
                .withInput(r1, r2, r3).withOutput(r1, r2, r3).runTest();
    }

    @Test
    public void testSelectRowIdNotFirstColumn()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addColumn("name", "metadata", "name", VARCHAR)
                .addRowId("foo", BIGINT);

        Row r1 = Row.newRow().addField("Alice", VARCHAR).addField(1, BIGINT);
        Row r2 = Row.newRow().addField("Bob", VARCHAR).addField(2, BIGINT);
        Row r3 = Row.newRow().addField("Carol", VARCHAR).addField(3, BIGINT);

        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema)
                .withRowIdColumnName("foo").withInput(r1, r2, r3).withOutput(r1, r2, r3).runTest();
    }

    @Test(expectedExceptions = SQLException.class)
    public void testErrorOnOnlyOneNullField()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR).addColumn("age",
                "metadata", "age", BIGINT);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(60L, BIGINT);
        Row r2 = Row.newRow().addField("row1", VARCHAR).addField(null, BIGINT);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable WHERE age = 0 OR age is NULL")
                .withInputSchema(schema).withInput(r1, r2).initialize();
    }

    @Test(expectedExceptions = SQLException.class)
    public void testErrorOnBothFieldsNull()
            throws Exception
    {
        RowSchema schema = RowSchema.newRowSchema().addRowId("recordkey", VARCHAR)
                .addColumn("age", "metadata", "age", BIGINT)
                .addColumn("male", "metadata", "male", BOOLEAN);

        Row r1 = Row.newRow().addField("row1", VARCHAR).addField(60L, BIGINT).addField(true,
                BOOLEAN);
        Row r2 = Row.newRow().addField("row1", VARCHAR).addField(null, BIGINT).addField(null,
                BOOLEAN);

        // no constraints on predicate pushdown
        HARNESS.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable")
                .withQuery("SELECT * FROM testmytable").withInputSchema(schema).withInput(r1, r2)
                .initialize();
    }

    private static Calendar c(int y, int m, int d)
    {
        return new GregorianCalendar(y, m - 1, d);
    }
}
