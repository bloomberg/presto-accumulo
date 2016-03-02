package bloomberg.presto.accumulo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * JSON object for holding the Accumulo ranges(s) and the host/port information for a Presto split
 * TODO Can this be rolled into an AccumuloSplit?
 */
public class TabletSplitMetadata
{
    private final String hostPort;
    private List<RangeHandle> rHandles;

    /**
     * JSON creator for a new instance of {@link TabletSplitMetadata}
     *
     * @param hostPort
     *            Host:port pair of the Accumulo tablet server
     * @param rHandles
     *            List of {@link RangeHandle} objects for a single split
     */
    @JsonCreator
    public TabletSplitMetadata(@JsonProperty("hostPort") String hostPort,
            @JsonProperty("rHandles") List<RangeHandle> rHandles)
    {
        this.hostPort = requireNonNull(hostPort, "hostPort is null");
        this.rHandles = rHandles;
    }

    /**
     * Gets the host:port string
     *
     * @return Host and port
     */
    @JsonProperty
    public String getHostPort()
    {
        return hostPort;
    }

    /**
     * Gets the list of {@link RangeHandle} objects
     *
     * @return List of range handles
     */
    @JsonProperty
    public List<RangeHandle> getRangeHandles()
    {
        return rHandles;
    }

    /**
     * Sets the list of range handles
     *
     * @param rHandles
     *            List of range handles
     */
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
        return Objects.equals(this.hostPort, other.hostPort)
                && Objects.equals(this.rHandles, other.rHandles);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("hostPort", hostPort).add("numHandles", rHandles.size())
                .toString();
    }
}
