package bloomberg.presto.accumulo;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

public class TabletSplitMetadata {
    private final byte[] split;
    private final String strSplit;
    private final String hostPort;
    private RangeHandle rHandle;

    @JsonCreator
    public TabletSplitMetadata(@JsonProperty("split") byte[] split,
            @JsonProperty("hostPort") String hostPort,
            @JsonProperty("rHandle") RangeHandle rHandle) {
        this.split = split;
        this.strSplit = this.split != null ? new String(this.split) : null;
        this.hostPort = requireNonNull(hostPort, "hostPort is null");
        this.rHandle = rHandle;
    }

    @JsonProperty
    public byte[] getSplit() {
        return split;
    }

    @JsonProperty
    public String getHostPort() {
        return hostPort;
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
        return Objects.hash(strSplit, hostPort);
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
        return Objects.equals(this.strSplit, other.strSplit)
                && Objects.equals(this.hostPort, other.hostPort)
                && Objects.equals(this.rHandle, other.rHandle);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("split", strSplit)
                .add("hostPort", hostPort).add("range", rHandle).toString();
    }
}
