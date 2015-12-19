package bloomberg.presto.accumulo;

import java.io.File;

import org.junit.Test;

import bloomberg.presto.accumulo.benchmark.QueryDriver;
import bloomberg.presto.accumulo.benchmark.Row;
import bloomberg.presto.accumulo.benchmark.RowSchema;

public class LargeDataTests {

    private File INPUT_FILE_A = new File("src/test/resources/file_a.txt");
    private final RowSchema SCHEMA = RowSchema.newInstance().addRowId()
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

    @Test
    public void testSelectCount() throws Exception {
        QueryDriver harness = new QueryDriver("default", "localhost:2181",
                "root", "secret");

        Row r1 = Row.newInstance().addField(10000, PrestoType.BIGINT);

        harness.withHost("localhost").withPort(8080).withSchema("default")
                .withTable("testmytable")
                .withQuery("SELECT COUNT(*) FROM testmytable")
                .withInputSchema(SCHEMA).withInputFile(INPUT_FILE_A)
                .withOutput(r1).runTest();
    }
}
