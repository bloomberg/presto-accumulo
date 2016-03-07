package bloomberg.presto.accumulo.iterators;

import bloomberg.presto.accumulo.iterators.SingleColumnValueFilter.CompareOp;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static bloomberg.presto.accumulo.iterators.AndFilter.andFilters;
import static bloomberg.presto.accumulo.iterators.SingleColumnValueFilter.getProperties;

public class AndFilterTest
{
    private static LexicoderRowSerializer serializer = new LexicoderRowSerializer();

    private static byte[] encode(Type type, Object v)
    {
        return serializer.encode(type, v);
    }

    IteratorSetting cq1gt3 = new IteratorSetting(1, "1", SingleColumnValueFilter.class,
            getProperties("cf1", "cq1", CompareOp.GREATER, encode(BigintType.BIGINT, 3L)));

    IteratorSetting cq1lt10 = new IteratorSetting(2, "2", SingleColumnValueFilter.class,
            getProperties("cf1", "cq1", CompareOp.LESS, encode(BigintType.BIGINT, 10L)));

    IteratorSetting cq2lt10 = new IteratorSetting(2, "2", SingleColumnValueFilter.class,
            getProperties("cf1", "cq2", CompareOp.LESS, encode(BigintType.BIGINT, 10L)));

    @Test
    public void testSameColumn()
            throws IOException
    {
        IteratorSetting settings = andFilters(3, cq1gt3, cq1lt10);

        TestKeyValueIterator iter =
                new TestKeyValueIterator(k("row1", "cf1", "cq1"), v(BigintType.BIGINT, 4L));

        AndFilter filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertTrue(filter.acceptRow(iter));

        iter.clear().add(k("row1", "cf1", "cq1"), v(BigintType.BIGINT, 3L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertFalse(filter.acceptRow(iter));

        iter.clear().add(k("row1", "cf1", "cq1"), v(BigintType.BIGINT, 10L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertFalse(filter.acceptRow(iter));
    }

    @Test
    public void testDifferentColumns()
            throws IOException
    {
        IteratorSetting settings = andFilters(1, cq1gt3, cq2lt10);

        TestKeyValueIterator iter = new TestKeyValueIterator();
        iter.add(k("row1", "cf1", "cq1"), v(BigintType.BIGINT, 4L));
        iter.add(k("row1", "cf1", "cq2"), v(BigintType.BIGINT, 9L));

        AndFilter filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertTrue(filter.acceptRow(iter));

        iter.clear().add(k("row1", "cf1", "cq1"), v(BigintType.BIGINT, 10L))
                .add(k("row1", "cf1", "cq2"), v(BigintType.BIGINT, 8L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertTrue(filter.acceptRow(iter));

        iter.clear().add(k("row1", "cf1", "cq1"), v(BigintType.BIGINT, 3L))
                .add(k("row1", "cf1", "cq2"), v(BigintType.BIGINT, 8L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertFalse(filter.acceptRow(iter));

        iter.clear().add(k("row1", "cf1", "cq1"), v(BigintType.BIGINT, 4L))
                .add(k("row1", "cf1", "cq2"), v(BigintType.BIGINT, 10L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertFalse(filter.acceptRow(iter));
    }

    @Test
    public void testNullColumn()
            throws IOException
    {
        IteratorSetting settings = andFilters(1, cq1gt3, cq2lt10);

        TestKeyValueIterator iter =
                new TestKeyValueIterator(k("row1", "cf1", "cq1"), v(BigintType.BIGINT, 4L));

        AndFilter filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertFalse(filter.acceptRow(iter));

        iter.clear().add(k("row1", "cf1", "cq2"), v(BigintType.BIGINT, 10L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        Assert.assertFalse(filter.acceptRow(iter));
    }

    private static Key k(String r, String f, String q)
    {
        return new Key(r, f, q);
    }

    private static Value v(Type t, Object o)
    {
        return new Value(encode(t, o));
    }
}
