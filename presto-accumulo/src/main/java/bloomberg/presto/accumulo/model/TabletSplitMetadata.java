package bloomberg.presto.accumulo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.accumulo.core.data.Range;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * JSON object for holding the Accumulo ranges(s) and the host/port information for a Presto split
 */
public class TabletSplitMetadata
{
    private final String hostPort;
    private List<Range> ranges;

    /**
     * JSON creator for a new instance of {@link TabletSplitMetadata}
     *
     * @param hostPort
     *            Host:port pair of the Accumulo tablet server
     * @param ranges
     *            List of Range objects for a single split
     */
    @JsonCreator
    public TabletSplitMetadata(@JsonProperty("hostPort") String hostPort,
            @JsonProperty("ranges") List<Range> ranges)
    {
        this.hostPort = requireNonNull(hostPort, "hostPort is null");
        this.ranges = ranges;
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
     * Gets the list of Range objects
     *
     * @return List of ranges
     */
    @JsonProperty
    public List<Range> getRanges()
    {
        return ranges;
    }

    /**
     * Sets the list of ra nge handles
     *
     * @param ranges
     *            List of range handles
     */
    @JsonSetter
    public void setRanges(List<Range> ranges)
    {
        this.ranges = ranges;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hostPort, ranges);
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
                && Objects.equals(this.ranges, other.ranges);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("hostPort", hostPort).add("numRanges", ranges.size())
                .toString();
    }
}
