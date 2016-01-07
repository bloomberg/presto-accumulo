package bloomberg.presto.accumulo;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

public class TabletSplitMetadata {
    private final String split;
    private final String host;
    private final int port;
    private RangeHandle rHandle;

    @JsonCreator
    public TabletSplitMetadata(@JsonProperty("split") String split,
            @JsonProperty("host") String host,
            @JsonProperty("port") Integer port,
            @JsonProperty("rHandle") RangeHandle rHandle) {
        this.split = split;
        this.host = requireNonNull(host, "host is null");
        this.port = requireNonNull(port, "port is null");
        this.rHandle = rHandle;
    }

    @JsonProperty
    public String getSplit() {
        return split;
    }

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public int getPort() {
        return port;
    }

    @JsonProperty
    public RangeHandle getRangeHandle() {
        return rHandle;
    }

    @JsonSetter
    public void setRangeHandle(RangeHandle rHandle) {
        this.rHandle = rHandle;
    }

    @Override
    public int hashCode() {
        return Objects.hash(split, host, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        TabletSplitMetadata other = (TabletSplitMetadata) obj;
        return Objects.equals(this.split, other.split)
                && Objects.equals(this.host, other.host)
                && Objects.equals(this.port, other.port)
                && Objects.equals(this.rHandle, other.rHandle);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("split", split).add("host", host)
                .add("port", port).add("range", rHandle).toString();
    }
}
