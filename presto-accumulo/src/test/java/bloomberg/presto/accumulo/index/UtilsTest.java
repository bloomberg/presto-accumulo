package bloomberg.presto.accumulo.index;

import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static bloomberg.presto.accumulo.serializers.LexicoderRowSerializer.encode;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.ByteBuffer.wrap;

public class UtilsTest
{
    private static final byte[] AGE = "age".getBytes();
    private static final byte[] CF = "cf".getBytes();
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] FIRSTNAME = "firstname".getBytes();

    @Test
    public void testMutationIndex()
    {
        ByteBuffer m1Row = wrap(encode(VARCHAR, "row1"));
        ByteBuffer m1Age = wrap(encode(BIGINT, 27L));
        ByteBuffer m1FirstName = wrap(encode(VARCHAR, "alice"));

        ByteBuffer m2Row = wrap(encode(VARCHAR, "row2"));
        ByteBuffer m2Age = wrap(encode(BIGINT, 27L));
        ByteBuffer m2FirstName = wrap(encode(VARCHAR, "bob"));

        Mutation m1 = new Mutation(m1Row.array());
        m1.put(CF, AGE, m1Age.array());
        m1.put(CF, FIRSTNAME, m1FirstName.array());

        Mutation m2 = new Mutation(m2Row.array());
        m2.put(CF, AGE, m2Age.array());
        m2.put(CF, FIRSTNAME, m2FirstName.array());

        Map<ByteBuffer, Set<ByteBuffer>> indexColumns = new HashMap<>();
        indexColumns.put(wrap(CF), ImmutableSet.of(wrap(AGE), wrap(FIRSTNAME)));

        List<Mutation> indexMutations = new ArrayList<>();
        Map<ByteBuffer, Map<ByteBuffer, AtomicLong>> metrics = Utils.getMetricsDataStructure();
        Utils.indexMutation(m1, indexColumns, indexMutations, metrics);
        Assert.assertEquals(2, indexMutations.size());
        Assert.assertEquals(3, metrics.size());

        Assert.assertArrayEquals(m1Age.array(), indexMutations.get(0).getRow());
        Assert.assertArrayEquals(m1FirstName.array(), indexMutations.get(1).getRow());

        ByteBuffer idxCfAge = Utils.getIndexColumnFamily(CF, AGE);
        ByteBuffer idxCfFirstName = Utils.getIndexColumnFamily(CF, FIRSTNAME);

        Mutation m1Idx1 = new Mutation(m1Age.array());
        m1Idx1.put(idxCfAge.array(), m1Row.array(), EMPTY_BYTES);
        Mutation m1Idx2 = new Mutation(m1FirstName.array());
        m1Idx2.put(idxCfFirstName.array(), m1Row.array(), EMPTY_BYTES);

        Assert.assertTrue(indexMutations.contains(m1Idx1));
        Assert.assertTrue(indexMutations.contains(m1Idx2));

        Assert.assertTrue(metrics.containsKey(m1Age));
        Assert.assertTrue(metrics.containsKey(m1FirstName));
        Assert.assertTrue(metrics.get(m1Age).containsKey(idxCfAge));
        Assert.assertTrue(metrics.get(m1FirstName).containsKey(idxCfFirstName));
        Assert.assertEquals(1, metrics.get(m1Age).get(idxCfAge).get());
        Assert.assertEquals(1, metrics.get(m1FirstName).get(idxCfFirstName).get());

        Utils.indexMutation(m2, indexColumns, indexMutations, metrics);
        Assert.assertEquals(4, indexMutations.size());
        Assert.assertEquals(4, metrics.size());

        Mutation m2Idx1 = new Mutation(m2Age.array());
        m2Idx1.put(idxCfAge.array(), m2Row.array(), EMPTY_BYTES);
        Mutation m2Idx2 = new Mutation(m2FirstName.array());
        m2Idx2.put(idxCfFirstName.array(), m2Row.array(), EMPTY_BYTES);

        Assert.assertTrue(indexMutations.contains(m2Idx1));
        Assert.assertTrue(indexMutations.contains(m2Idx2));

        Assert.assertTrue(metrics.containsKey(m1Age));
        Assert.assertTrue(metrics.containsKey(m1FirstName));
        Assert.assertTrue(metrics.containsKey(m2FirstName));
        Assert.assertTrue(metrics.get(m1Age).containsKey(idxCfAge));
        Assert.assertTrue(metrics.get(m1FirstName).containsKey(idxCfFirstName));
        Assert.assertTrue(metrics.get(m2FirstName).containsKey(idxCfFirstName));
        Assert.assertEquals(2, metrics.get(m1Age).get(idxCfAge).get());
        Assert.assertEquals(1, metrics.get(m1FirstName).get(idxCfFirstName).get());
        Assert.assertEquals(1, metrics.get(m2FirstName).get(idxCfFirstName).get());
    }
}
