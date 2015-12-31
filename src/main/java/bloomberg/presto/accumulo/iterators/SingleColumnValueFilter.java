package bloomberg.presto.accumulo.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableMap;

import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;

public class SingleColumnValueFilter extends Filter {

    private static final Logger LOG = Logger
            .getLogger(SingleColumnValueFilter.class);

    public enum CompareOp {
        LESS, LESS_OR_EQUAL, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, GREATER, NO_OP,
    }

    protected static final String CF = "family";
    protected static final String CQ = "qualifier";
    protected static final String COMPARE_OP = "compareOp";
    protected static final String VALUE = "value";

    private Text columnFamily;
    private Text columnQualifier;
    private Value value;
    private CompareOp compareOp;
    private Text cf = new Text();
    private Text cq = new Text();

    public static Map<String, String> getProperties(String family,
            String qualifier, CompareOp op, byte[] value) {

        Map<String, String> opts = new HashMap<>();

        opts.put(CF, family);
        opts.put(CQ, qualifier);
        opts.put(COMPARE_OP, op.toString());
        opts.put(VALUE, Hex.encodeHexString(value));

        return opts;
    }

    @Override
    public boolean accept(Key k, Value v) {
        boolean c = acceptHelper(k, v);
        if (c) {
            LOG.info(String.format("ACCEPT %s %s", k, v));
        } else {
            LOG.info(String.format("REJECT %s %s", k, v));
        }
        return c;
    }

    private boolean acceptHelper(Key k, Value v) {
        if (compareOp == CompareOp.NO_OP) {
            return true;
        }

        k.getColumnFamily(cf);
        if (columnFamily.equals(cf)) {
            k.getColumnQualifier(cq);
            if (columnQualifier.equals(cq)) {
                int compareResult = v.compareTo(value);
                LOG.info(String.format(
                        "%s COMPARE OF VALUE %s AGAINST %s IS %d", compareOp,
                        LexicoderRowSerializer.getLexicoder(BigintType.BIGINT)
                                .decode(v.get()),
                        LexicoderRowSerializer.getLexicoder(BigintType.BIGINT)
                                .decode(value.get()),
                        compareResult));
                switch (compareOp) {
                case LESS:
                    return compareResult < 0;
                case LESS_OR_EQUAL:
                    return compareResult <= 0;
                case EQUAL:
                    return compareResult == 0;
                case NOT_EQUAL:
                    return compareResult != 0;
                case GREATER_OR_EQUAL:
                    return compareResult >= 0;
                case GREATER:
                    return compareResult > 0;
                default:
                    throw new RuntimeException(
                            "Unknown Compare op " + compareOp.name());
                }
            }
        }
        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source,
            Map<String, String> options, IteratorEnvironment env)
                    throws IOException {
        super.init(source, options, env);
        columnFamily = new Text(options.get(CF));
        columnQualifier = new Text(options.get(CQ));
        compareOp = CompareOp.valueOf(options.get(COMPARE_OP));
        try {
            value = new Value(Hex.decodeHex(options.get(VALUE).toCharArray()));
        } catch (DecoderException e) {
            // should not occur, as validateOptions tries this same thing
            throw new IllegalArgumentException(
                    "Error decoding hex value in option", e);
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(
            IteratorEnvironment env) {

        // Create a new SingleColumnValueFilter object based on the parent's
        // deepCopy
        SingleColumnValueFilter copy = (SingleColumnValueFilter) super.deepCopy(
                env);

        // Replicate all of the current options into the copy
        copy.columnFamily = new Text(this.columnFamily);
        copy.columnQualifier = new Text(this.columnQualifier);
        copy.value = new Value(this.value);
        copy.compareOp = this.compareOp;

        // Return the copy
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("singlecolumnvaluefilter",
                "Filter accepts or rejects each Key/Value pair based on the lexicographic comparison of a value stored in a single column family/qualifier",
                ImmutableMap.of(
                        // @formatter:off
                        CF, "column family to match on, required",
                        CQ, "column qualifier to match on, required",
                        COMPARE_OP, "CompareOp enum type for lexicographic comparison, required",
                        VALUE, "Hex-encoded bytes of the value for comparison, required",
                        NEGATE, "default false keeps k/v that pass accept method, true rejects k/v that pass accept method"),
                        // @formatter:on
                null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        super.validateOptions(options);
        LOG.info("validate " + options);
        checkNotNull(CF, options);
        checkNotNull(CQ, options);
        checkNotNull(COMPARE_OP, options);
        checkNotNull(VALUE, options);

        try {
            CompareOp.valueOf(options.get(COMPARE_OP));
            LOG.info("compare op configured");
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unknown value of " + COMPARE_OP
                    + ":" + options.get(COMPARE_OP));
        }

        try {
            new Value(Hex.decodeHex(options.get(VALUE).toCharArray()));
            LOG.info("value configured");
        } catch (DecoderException e) {
            throw new IllegalArgumentException("Option " + VALUE
                    + " is not a hex-encoded value: " + options.get(VALUE), e);
        }
        return true;
    }

    private void checkNotNull(String opt, Map<String, String> options) {
        if (options.get(opt) == null) {
            throw new IllegalArgumentException(
                    "Option " + opt + " is required");
        } else {
            LOG.info(opt + " configured");
        }
    }
}