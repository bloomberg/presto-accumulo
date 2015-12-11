package bloomberg.presto.accumulo;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.accumulo.core.data.Range;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class RangeHandle {
    private final String startKey;
    private final Boolean startKeyInclusive;
    private final String endKey;
    private final Boolean endKeyInclusive;

    @JsonCreator
    public RangeHandle(@JsonProperty("startKey") String startKey,
            @JsonProperty("startKeyInclusive") Boolean startKeyInclusive,
            @JsonProperty("endKey") String endKey,
            @JsonProperty("endKeyInclusive") Boolean endKeyInclusive) {
        this.startKey = startKey;
        this.startKeyInclusive = requireNonNull(startKeyInclusive,
                "startKeyInclusive is null");
        this.endKey = endKey;
        this.endKeyInclusive = requireNonNull(endKeyInclusive,
                "endKeyInclusive is null");
    }

    @JsonProperty
    public String getStartKey() {
        return startKey;
    }

    @JsonProperty
    public Boolean getStartKeyInclusive() {
        return startKeyInclusive;
    }

    @JsonProperty
    public String getEndKey() {
        return endKey;
    }

    @JsonProperty
    public Boolean getEndKeyInclusive() {
        return endKeyInclusive;
    }

    @JsonIgnore
    public Range getRange() {
        return new Range(startKey, startKeyInclusive, endKey, endKeyInclusive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKey, startKeyInclusive, endKey,
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
        return Objects.equals(this.startKey, other.startKey)
                && Objects.equals(this.startKeyInclusive,
                        other.startKeyInclusive)
                && Objects.equals(this.endKey, other.endKey)
                && Objects.equals(this.endKeyInclusive, other.endKeyInclusive);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("startKey", startKey)
                .add("startKeyInclusive", startKeyInclusive)
                .add("endKey", endKey).add("endKeyInclusive", endKeyInclusive)
                .toString();
    }
}
