package bloomberg.presto.accumulo;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class RangeHandle {
    private final byte[] startKey;
    private final Text startKeyText;
    private final Boolean startKeyInclusive;
    private final byte[] endKey;
    private final Text endKeyText;
    private final Boolean endKeyInclusive;

    @JsonCreator
    public RangeHandle(@JsonProperty("startKey") byte[] startKey,
            @JsonProperty("startKeyInclusive") Boolean startKeyInclusive,
            @JsonProperty("endKey") byte[] endKey,
            @JsonProperty("endKeyInclusive") Boolean endKeyInclusive) {
        this.startKey = startKey;
        this.startKeyText = startKey != null ? new Text(startKey) : null;
        this.startKeyInclusive = requireNonNull(startKeyInclusive,
                "startKeyInclusive is null");
        this.endKey = endKey;
        this.endKeyText = endKey != null ? new Text(endKey) : null;
        this.endKeyInclusive = requireNonNull(endKeyInclusive,
                "endKeyInclusive is null");
    }

    @JsonProperty
    public byte[] getStartKey() {
        return startKey;
    }

    @JsonProperty
    public Boolean getStartKeyInclusive() {
        return startKeyInclusive;
    }

    @JsonProperty
    public byte[] getEndKey() {
        return endKey;
    }

    @JsonProperty
    public Boolean getEndKeyInclusive() {
        return endKeyInclusive;
    }

    @JsonIgnore
    public Range getRange() {
        if (startKeyText == null && endKeyText == null) {
            return new Range();
        } else if (startKeyText != null && endKeyText != null
                && startKeyText.equals(endKeyText)) {
            return new Range(startKeyText);
        } else {
            return new Range(startKeyText, startKeyInclusive, endKeyText,
                    endKeyInclusive);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKeyText, startKeyInclusive, endKeyText,
                endKeyInclusive);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        RangeHandle other = (RangeHandle) obj;
        return Objects.equals(this.startKeyText, other.startKeyText)
                && Objects.equals(this.startKeyInclusive,
                        other.startKeyInclusive)
                && Objects.equals(this.endKeyText, other.endKeyText)
                && Objects.equals(this.endKeyInclusive, other.endKeyInclusive);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("startKey", startKeyText)
                .add("startKeyInclusive", startKeyInclusive)
                .add("endKey", endKeyText)
                .add("endKeyInclusive", endKeyInclusive).toString();
    }
}
