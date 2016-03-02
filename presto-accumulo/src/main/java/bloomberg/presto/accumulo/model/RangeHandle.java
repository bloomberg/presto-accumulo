package bloomberg.presto.accumulo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * JSON object wrapper for an Accumulo Range
 * // TODO Could probably just use a custom jackson serializer
 */
public final class RangeHandle
{
    private final byte[] startKey;
    private final Text startKeyText;
    private final Boolean startKeyInclusive;
    private final byte[] endKey;
    private final Text endKeyText;
    private final Boolean endKeyInclusive;

    /**
     * JSON creator for a new instance of {@link RangeHandle}
     *
     * @param startKey
     *            Start key
     * @param startKeyInclusive
     *            Start key inclusive
     * @param endKey
     *            end
     * @param endKeyInclusive
     *            End key inclusive
     */
    @JsonCreator
    public RangeHandle(@JsonProperty("startKey") byte[] startKey,
            @JsonProperty("startKeyInclusive") Boolean startKeyInclusive,
            @JsonProperty("endKey") byte[] endKey,
            @JsonProperty("endKeyInclusive") Boolean endKeyInclusive)
    {
        this.startKey = startKey;
        this.startKeyText = startKey != null ? new Text(startKey) : null;
        this.startKeyInclusive = requireNonNull(startKeyInclusive, "startKeyInclusive is null");
        this.endKey = endKey;
        this.endKeyText = endKey != null ? new Text(endKey) : null;
        this.endKeyInclusive = requireNonNull(endKeyInclusive, "endKeyInclusive is null");
    }

    /**
     * Gets the start key bytes
     *
     * @return bytes
     */
    @JsonProperty
    public byte[] getStartKey()
    {
        return startKey;
    }

    /**
     * Gets a Boolean value indicating whether or not the start key is inclusive
     *
     * @return True if inclusive, false otherwise
     */
    @JsonProperty
    public Boolean getStartKeyInclusive()
    {
        return startKeyInclusive;
    }

    /**
     * Gets the end key bytes
     *
     * @return bytes
     */
    @JsonProperty
    public byte[] getEndKey()
    {
        return endKey;
    }

    /**
     * Gets a Boolean value indicating whether or not the end key is inclusive
     *
     * @return True if inclusive, false otherwise
     */
    @JsonProperty
    public Boolean getEndKeyInclusive()
    {
        return endKeyInclusive;
    }

    /**
     * Gets the Accumulo Range from this handle
     *
     * @return Accumulo Range
     */
    @JsonIgnore
    public Range getRange()
    {
        if (startKeyText == null && endKeyText == null) {
            return new Range();
        }
        else if (startKeyText != null && endKeyText != null && startKeyText.equals(endKeyText)) {
            return new Range(startKeyText);
        }
        else {
            return new Range(startKeyText, startKeyInclusive, endKeyText, endKeyInclusive);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(startKeyText, startKeyInclusive, endKeyText, endKeyInclusive);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        RangeHandle other = (RangeHandle) obj;
        return Objects.equals(this.startKeyText, other.startKeyText)
                && Objects.equals(this.startKeyInclusive, other.startKeyInclusive)
                && Objects.equals(this.endKeyText, other.endKeyText)
                && Objects.equals(this.endKeyInclusive, other.endKeyInclusive);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("startKey", startKeyText)
                .add("startKeyInclusive", startKeyInclusive).add("endKey", endKeyText)
                .add("endKeyInclusive", endKeyInclusive).toString();
    }

    /**
     * Static function to convert an Accumulo Range into a {@link RangeHandle}
     *
     * @param range
     *            Range to convert
     * @return Range handle
     */
    public static RangeHandle from(Range range)
    {
        byte[] startKey =
                range.getStartKey() != null ? range.getStartKey().getRow().copyBytes() : null;
        byte[] endKey = range.getEndKey() != null ? range.getEndKey().getRow().copyBytes() : null;
        return new RangeHandle(startKey, range.isStartKeyInclusive(), endKey,
                range.isEndKeyInclusive());
    }
}
