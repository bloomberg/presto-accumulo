package bloomberg.presto.accumulo.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;

import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;

public class SingleColumnValueFilter extends RowFilter
        implements OptionDescriber {

    private static final Logger LOG = Logger
            .getLogger(SingleColumnValueFilter.class);

    public enum CompareOp {
        LESS, LESS_OR_EQUAL, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, GREATER, NO_OP,
    }

    protected static final String CF = "family";
    protected static final String CQ = "qualifier";
    protected static final String COMPARE_OP = "compareOp";
    protected static final String TYPE = "type";
    protected static final String VALUE = "value";

    private Text columnFamily;
    private Text columnQualifier;
    private Value value;
    private CompareOp compareOp;
    private Type type;
    private Text cf = new Text();
    private Text cq = new Text();

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key, Value> rowIterator)
            throws IOException {
        while (rowIterator.hasTop()) {
            if (!acceptSingleKeyValue(rowIterator.getTopKey(),
                    rowIterator.getTopValue())) {
                return false;
            }
            rowIterator.next();
        }
        return true;
    }

    private boolean acceptSingleKeyValue(Key k, Value v) {
        LOG.debug(String.format("Key: %s Value: %s", k, v));
        if (compareOp == CompareOp.NO_OP) {
            return true;
        }

        k.getColumnFamily(cf);
        if (columnFamily.equals(cf)) {
            k.getColumnQualifier(cq);
            if (columnQualifier.equals(cq)) {
                int compareResult = v.compareTo(value);
                if (type.equals(VarcharType.VARCHAR)) {
                    String tv = (String) LexicoderRowSerializer
                            .getLexicoder(type).decode(v.get());
                    String fv = (String) LexicoderRowSerializer
                            .getLexicoder(type).decode(value.get());
                    LOG.debug(String.format(
                            "%s COMPARE OF VALUE %s AGAINST %s IS %d",
                            compareOp, tv, fv, compareResult));
                } else if (type.equals(VarbinaryType.VARBINARY)) {
                    LOG.debug(String.format(
                            "%s COMPARE OF VALUE %s AGAINST %s IS %d",
                            compareOp,
                            Hex.encodeHexString((byte[]) LexicoderRowSerializer
                                    .getLexicoder(type).decode(v.get())),
                            Hex.encodeHexString((byte[]) LexicoderRowSerializer
                                    .getLexicoder(type).decode(value.get())),
                            compareResult));
                } else {
                    LOG.debug(String.format(
                            "%s COMPARE OF VALUE %s AGAINST %s IS %d",
                            compareOp,
                            LexicoderRowSerializer.getLexicoder(type)
                                    .decode(v.get()),
                            LexicoderRowSerializer.getLexicoder(type)
                                    .decode(value.get()),
                            compareResult));
                }
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
        TypeRegistry t = new TypeRegistry();
        type = t.getType(TypeSignature.parseTypeSignature(options.get(TYPE)));

        if (type == null) {
            throw new IllegalArgumentException(
                    "Type is null from options " + options.get(TYPE));
        }

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
        SingleColumnValueFilter copy = new SingleColumnValueFilter();

        // Replicate all of the current options into the copy
        copy.columnFamily = new Text(this.columnFamily);
        copy.columnQualifier = new Text(this.columnQualifier);
        copy.value = new Value(this.value);
        copy.type = type;
        copy.compareOp = this.compareOp;

        // Return the copy
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {

        return new IteratorOptions("singlecolumnvaluefilter",
                "Filter accepts or rejects each Key/Value pair based on the lexicographic comparison of a value stored in a single column family/qualifier",
                // @formatter:off
                    ImmutableMap.<String, String>builder().put(CF, "column family to match on, required")
                        .put(CQ, "column qualifier to match on, required")
                        .put(COMPARE_OP, "CompareOp enum type for lexicographic comparison, required")
                        .put(TYPE, "Presto Type for logging, required")
                        .put(VALUE, "Hex-encoded bytes of the value for comparison, required")
                        .build(),
                // @formatter:on
                null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        checkNotNull(CF, options);
        checkNotNull(CQ, options);
        checkNotNull(COMPARE_OP, options);
        checkNotNull(TYPE, options);
        checkNotNull(VALUE, options);

        try {
            CompareOp.valueOf(options.get(COMPARE_OP));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unknown value of " + COMPARE_OP
                    + ":" + options.get(COMPARE_OP));
        }

        try {
            new Value(Hex.decodeHex(options.get(VALUE).toCharArray()));
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
        }
    }

    public static Map<String, String> getProperties(String family,
            String qualifier, CompareOp op, Type type, byte[] value) {

        Map<String, String> opts = new HashMap<>();

        opts.put(CF, family);
        opts.put(CQ, qualifier);
        opts.put(COMPARE_OP, op.toString());
        opts.put(TYPE, type.getDisplayName());
        opts.put(VALUE, Hex.encodeHexString(value));

        return opts;
    }

}