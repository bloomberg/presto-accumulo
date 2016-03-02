package bloomberg.presto.accumulo.integration.tests;

import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.model.RowSchema;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.VarcharType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.sql.Date;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class LargeDataTest
{
    private static final File INPUT_FILE = new File("src/test/resources/datagen.txt.gz");
    private static final File OTHER_INPUT_FILE = new File("src/test/resources/datagen-other.txt.gz");
    private static final File FIRST_NAME_SELECT_OUTPUT = new File("src/test/resources/first_name_select.txt.gz");

    private static final RowSchema INPUT_SCHEMA = RowSchema.newRowSchema()
            .addRowId()
            .addColumn("first_name", "metadata", "first_name", VarcharType.VARCHAR)
            .addColumn("last_name", "metadata", "last_name", VarcharType.VARCHAR)
            .addColumn("address", "metadata", "address", VarcharType.VARCHAR)
            .addColumn("city", "metadata", "city", VarcharType.VARCHAR)
            .addColumn("state", "metadata", "state", VarcharType.VARCHAR)
            .addColumn("zipcode", "metadata", "zipcode", BigintType.BIGINT)
            .addColumn("birthday", "metadata", "birthday", DateType.DATE)
            .addColumn("favorite_color", "metadata", "favorite_color", VarcharType.VARCHAR);

    private static final RowSchema INDEXED_SCHEMA = RowSchema.newRowSchema()
            .addRowId()
            .addColumn("first_name", "metadata", "first_name", VarcharType.VARCHAR, true)
            .addColumn("last_name", "metadata", "last_name", VarcharType.VARCHAR)
            .addColumn("address", "metadata", "address", VarcharType.VARCHAR)
            .addColumn("city", "metadata", "city", VarcharType.VARCHAR)
            .addColumn("state", "metadata", "state", VarcharType.VARCHAR)
            .addColumn("zipcode", "metadata", "zipcode", BigintType.BIGINT)
            .addColumn("birthday", "metadata", "birthday", DateType.DATE)
            .addColumn("favorite_color", "metadata", "favorite_color", VarcharType.VARCHAR);

    private static final Integer NUM_RECORDS = 100000;
    private static final QueryDriver DRIVER1;
    private static final QueryDriver DRIVER2;
    private static final QueryDriver DRIVER3;

    private static final AccumuloConfig ACCUMULO_CONFIG = new AccumuloConfig();

    static {
        try {
            ACCUMULO_CONFIG.setInstance("default");
            ACCUMULO_CONFIG.setZooKeepers("localhost:2181");
            ACCUMULO_CONFIG.setUsername("root");
            ACCUMULO_CONFIG.setPassword("secret");
            DRIVER1 = new QueryDriver(ACCUMULO_CONFIG);
            DRIVER2 = new QueryDriver(ACCUMULO_CONFIG);
            DRIVER3 = new QueryDriver(ACCUMULO_CONFIG);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setupClass()
            throws Exception
    {
        DRIVER1.withHost("localhost").withPort(8080).withSchema("default").withTable("testmytable").withInputSchema(INPUT_SCHEMA).withInputFile(INPUT_FILE).initialize();

        DRIVER2.withHost("localhost").withPort(8080).withSchema("default").withTable("testmyothertable").withInputSchema(INPUT_SCHEMA).withInputFile(OTHER_INPUT_FILE).initialize();

        DRIVER3.withHost("localhost").withPort(8080).withSchema("default").withTable("testmyindexedtable").withInputSchema(INDEXED_SCHEMA).withInputFile(INPUT_FILE).initialize();
    }

    @AfterClass
    public static void cleanupClass()
            throws Exception
    {
        DRIVER1.cleanup();
        DRIVER2.cleanup();
        DRIVER3.cleanup();
    }

    @Test
    public void testSelectCount()
            throws Exception
    {
        Row r1 = Row.newRow().addField(NUM_RECORDS, BigintType.BIGINT);
        DRIVER1.withQuery("SELECT COUNT(*) FROM testmytable").withOutput(r1).runTest();
    }

    @Test
    public void testSelectWhereFirstNameIn()
            throws Exception
    {
        String query = "SELECT * FROM testmytable " + "WHERE first_name in ('Darla')";

        DRIVER1.withOutputSchema(INPUT_SCHEMA).withQuery(query).withOutputFile(FIRST_NAME_SELECT_OUTPUT).runTest();
    }

    @Test
    public void testSelectWhereFirstNameEquals()
            throws Exception
    {
        String query = "SELECT * FROM testmytable " + "WHERE first_name = 'Darla'";

        DRIVER1.withOutputSchema(INPUT_SCHEMA).withQuery(query).withOutputFile(FIRST_NAME_SELECT_OUTPUT).runTest();
    }

    @Test
    public void testSelectWhereFirstNameEqualsWithIndex()
            throws Exception
    {
        String query = "SELECT * FROM testmyindexedtable " + "WHERE first_name = 'Darla'";

        DRIVER1.withOutputSchema(INDEXED_SCHEMA).withQuery(query).withOutputFile(FIRST_NAME_SELECT_OUTPUT).runTest();
    }

    @Test
    public void testSelectCountMinMaxWhereFirstNameEquals()
            throws Exception
    {
        Row r1 = Row.newRow().addField(13L, BigintType.BIGINT).addField(new Date(73859156000L), DateType.DATE).addField(new Date(1328445195000L), DateType.DATE);

        String query = "SELECT COUNT(*) AS count, MIN(birthday), MAX(birthday) FROM testmytable " + "WHERE first_name = 'Darla'";

        DRIVER1.withQuery(query).withOutput(r1).runTest();
    }

    @Test
    public void testJoin()
            throws Exception
    {
        Row r1 = Row.newRow().addField(6L, BigintType.BIGINT);

        String query = "SELECT COUNT(*) AS count FROM testmytable tmt, testmyothertable tmot " + "WHERE tmt.zipcode = tmot.zipcode AND " + "tmt.birthday = tmot.birthday";

        DRIVER1.withQuery(query).withOutput(r1).runTest();
    }
}
