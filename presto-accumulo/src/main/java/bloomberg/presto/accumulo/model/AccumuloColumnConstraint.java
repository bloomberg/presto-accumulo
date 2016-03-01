package bloomberg.presto.accumulo.model;

import com.facebook.presto.spi.predicate.Domain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Jackson object for encapsulating a constraint on a column. Contains the Presto name of the
 * column, the Accumulo column family/qualifier, whether or not the column is indexed, and the
 * predicate Domain from the query.
 */
public class AccumuloColumnConstraint
{
    private final String name;
    private final String family;
    private final String qualifier;
    private final boolean indexed;
    private Domain domain;

    /**
     * JSON creator for an {@link AccumuloColumnConstraint}
     *
     * @param name
     *            Presto column name
     * @param family
     *            Accumulo column family
     * @param qualifier
     *            Accumulo column qualifier
     * @param domain
     *            Presto Domain for the column constraint
     * @param indexed
     *            True if the column is indexed, false otherwise
     */
    @JsonCreator
    public AccumuloColumnConstraint(@JsonProperty("name") String name,
            @JsonProperty("family") String family, @JsonProperty("qualifier") String qualifier,
            @JsonProperty("domain") Domain domain, @JsonProperty("indexed") boolean indexed)
    {
        this.name = requireNonNull(name, "name is null");
        this.family = requireNonNull(family, "family is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        this.indexed = requireNonNull(indexed, "indexed is null");

        // We don't require this to be null, as JSON does a top-down approach when deserializing
        // JSON objects. Will throw an exception during object resolution.
        this.domain = domain;
    }

    /**
     * Gets a Boolean value indicating whether or not this column contains entries in the index
     * table. This is a JSON property.
     *
     * @return True if indexed, false otherwise
     */
    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    /**
     * Gets the Presto column name. This is a JSON property.
     *
     * @return Presto column name
     */
    @JsonProperty
    public String getName()
    {
        return name;
    }

    /**
     * Gets the Accumulo column family. This is a JSON property.
     *
     * @return Column family
     */
    @JsonProperty
    public String getFamily()
    {
        return family;
    }

    /**
     * Gets the Accumulo column qualifier. This is a JSON property.
     *
     * @return Column qualifier
     */
    @JsonProperty
    public String getQualifier()
    {
        return qualifier;
    }

    /**
     * Gets the Presto predicate Domain for this column. This is a JSON property.
     *
     * @return Column qualifier
     */
    @JsonProperty
    public Domain getDomain()
    {
        return domain;
    }

    /**
     * Sets the Presto predicate Domain for this column. This is a JSON setter function.
     *
     * @param domain
     *            Column domain
     */
    @JsonSetter
    public void setDomain(Domain domain)
    {
        this.domain = domain;
    }

    public String toString()
    {
        return toStringHelper(this).add("name", this.name).add("family", this.family)
                .add("qualifier", this.qualifier).add("indexed", this.indexed)
                .add("domain", this.domain).toString();
    }
}
