package bloomberg.presto.accumulo.index;

import static bloomberg.presto.accumulo.serializers.LexicoderRowSerializer.encode;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Mutation;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class UtilsTest {

    private static final byte[] AGE = "age".getBytes();
    private static final byte[] CF = "cf".getBytes();
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] FIRSTNAME = "firstname".getBytes();

    @Test
    public void testMutationIndex() {

        Mutation m1 = new Mutation(encode(VARCHAR, "row1"));
        m1.put(CF, AGE, encode(BIGINT, 27L));
        m1.put(CF, FIRSTNAME, encode(VARCHAR, "alice"));

        Mutation m2 = new Mutation(encode(VARCHAR, "row2"));
        m2.put(CF, AGE, encode(BIGINT, 27L));
        m2.put(CF, FIRSTNAME, encode(VARCHAR, "bob"));

        Map<ByteBuffer, Set<ByteBuffer>> indexColumns = new HashMap<>();
        indexColumns.put(ByteBuffer.wrap(CF), ImmutableSet
                .of(ByteBuffer.wrap(AGE), ByteBuffer.wrap(FIRSTNAME)));

        List<Mutation> indexMutations = new ArrayList<>();
        Utils.indexMutation(m1, indexColumns, indexMutations);
        Assert.assertEquals(2, indexMutations.size());

        Assert.assertArrayEquals(encode(BIGINT, 27L),
                indexMutations.get(0).getRow());
        Assert.assertArrayEquals(encode(VARCHAR, "alice"),
                indexMutations.get(1).getRow());

        Mutation m1Idx1 = new Mutation(encode(BIGINT, 27L));
        m1Idx1.put(format("%s_%s", new String(CF), new String(AGE)).getBytes(),
                encode(VARCHAR, "row1"), EMPTY_BYTES);
        Mutation m1Idx2 = new Mutation(encode(VARCHAR, "alice"));
        m1Idx2.put(format("%s_%s", new String(CF), new String(FIRSTNAME))
                .getBytes(), encode(VARCHAR, "row1"), EMPTY_BYTES);

        Assert.assertTrue(indexMutations.contains(m1Idx1));
        Assert.assertTrue(indexMutations.contains(m1Idx2));

        Utils.indexMutation(m2, indexColumns, indexMutations);
        Assert.assertEquals(4, indexMutations.size());

        Mutation m2Idx1 = new Mutation(encode(BIGINT, 27L));
        m2Idx1.put(format("%s_%s", new String(CF), new String(AGE)).getBytes(),
                encode(VARCHAR, "row2"), EMPTY_BYTES);
        Mutation m2Idx2 = new Mutation(encode(VARCHAR, "bob"));
        m2Idx2.put(format("%s_%s", new String(CF), new String(FIRSTNAME))
                .getBytes(), encode(VARCHAR, "row2"), EMPTY_BYTES);

        Assert.assertTrue(indexMutations.contains(m2Idx1));
        Assert.assertTrue(indexMutations.contains(m2Idx2));

    }
}
