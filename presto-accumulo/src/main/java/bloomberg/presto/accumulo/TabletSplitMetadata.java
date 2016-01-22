package bloomberg.presto.accumulo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TabletSplitMetadata
{
    private final String hostPort;
    private List<RangeHandle> rHandles;

    @JsonCreator
    public TabletSplitMetadata(@JsonProperty("hostPort") String hostPort, @JsonProperty("rHandles") List<RangeHandle> rHandles)
    {
        this.hostPort = requireNonNull(hostPort, "hostPort is null");
        this.rHandles = rHandles;
    }

    @JsonProperty
    public String getHostPort()
    {
        return hostPort;
    }

    @JsonProperty
    public List<RangeHandle> getRangeHandles()
    {
        return rHandles;
    }

    @JsonSetter
    public void setRangeHandles(List<RangeHandle> rHandles)
    {
        this.rHandles = rHandles;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hostPort, rHandles);
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

        TabletSplitMetadata other = (TabletSplitMetadata) obj;
        return Objects.equals(this.hostPort, other.hostPort) && Objects.equals(this.rHandles, other.rHandles);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("hostPort", hostPort).add("rHandles", rHandles).toString();
    }
}
