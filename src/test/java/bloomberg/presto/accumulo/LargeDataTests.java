package bloomberg.presto.accumulo;

import java.io.File;
import java.sql.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import bloomberg.presto.accumulo.benchmark.QueryDriver;
import bloomberg.presto.accumulo.benchmark.RowSchema;
import bloomberg.presto.accumulo.storage.Row;

public class LargeDataTests {

    private static final File INPUT_FILE = new File(
            "src/test/resources/datagen.txt.gz");
    private static final File FIRST_NAME_SELECT_OUTPUT = new File(
            "src/test/resources/first_name_select.txt.gz");

    private static final RowSchema INPUT_SCHEMA = RowSchema.newInstance()
            .addRowId()
            .addColumn("first_name", "metadata", "first_name",
                    PrestoType.VARCHAR)
            .addColumn("last_name", "metadata", "last_name", PrestoType.VARCHAR)
            .addColumn("address", "metadata", "address", PrestoType.VARCHAR)
            .addColumn("city", "metadata", "city", PrestoType.VARCHAR)
            .addColumn("state", "metadata", "state", PrestoType.VARCHAR)
            .addColumn("zipcode", "metadata", "zipcode", PrestoType.BIGINT)
            .addColumn("birthday", "metadata", "birthday", PrestoType.DATE)
            .addColumn("favorite_color", "metadata", "favorite_color",
                    PrestoType.VARCHAR);

    private static final Integer NUM_RECORDS = 100000;
    private static final QueryDriver HARNESS;

    static {
        try {
            HARNESS = new QueryDriver("default", "localhost:2181", "root",
                    "secret");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setupClass() throws Exception {
        HARNESS.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable").withInputSchema(INPUT_SCHEMA)
                .withInputFile(INPUT_FILE).initialize();
    }

    @AfterClass
    public static void cleanupClass() throws Exception {
        HARNESS.cleanup();
    }

    @Test
    public void testSelectCount() throws Exception {
        Row r1 = Row.newInstance().addField(NUM_RECORDS, PrestoType.BIGINT);
        HARNESS.withQuery("SELECT COUNT(*) FROM testmytable").withOutput(r1)
                .runTest();
    }

    @Test
    public void testSelectWhereFirstNameIn() throws Exception {

        String query = "SELECT * FROM testmytable "
                + "WHERE first_name in ('Darla')";

        HARNESS.withOutputSchema(INPUT_SCHEMA).withQuery(query)
                .withOutputFile(FIRST_NAME_SELECT_OUTPUT).runTest();
    }

    @Test
    public void testSelectWhereFirstNameEquals() throws Exception {

        String query = "SELECT * FROM testmytable "
                + "WHERE first_name = 'Darla'";

        HARNESS.withOutputSchema(INPUT_SCHEMA).withQuery(query)
                .withOutputFile(FIRST_NAME_SELECT_OUTPUT).runTest();
    }

    @Test
    public void testSelectCountMinMaxWhereFirstNameEquals() throws Exception {

        Row r1 = Row.newInstance().addField(13L, PrestoType.BIGINT)
                .addField(new Date(73859156), PrestoType.DATE)
                .addField(new Date(1328445195), PrestoType.DATE);

        String query = "SELECT COUNT(*) AS count, MIN(birthday), MAX(birthday) FROM testmytable "
                + "WHERE first_name = 'Darla'";

        HARNESS.withQuery(query).withOutput(r1).runTest();
    }
}
