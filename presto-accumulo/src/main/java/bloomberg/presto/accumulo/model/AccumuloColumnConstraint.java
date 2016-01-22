package bloomberg.presto.accumulo.model;

import com.facebook.presto.spi.predicate.Domain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AccumuloColumnConstraint
{
    private final String name;
    private final String family;
    private final String qualifier;
    private final boolean indexed;
    private Domain domain;

    @JsonCreator
    public AccumuloColumnConstraint(@JsonProperty("name") String name, @JsonProperty("family") String family, @JsonProperty("qualifier") String qualifier, @JsonProperty("domain") Domain domain, @JsonProperty("indexed") boolean indexed)
    {
        this.name = requireNonNull(name, "name is null");
        this.family = requireNonNull(family, "family is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        this.indexed = requireNonNull(indexed, "indexed is null");
        this.domain = domain;
    }

    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getFamily()
    {
        return family;
    }

    @JsonProperty
    public String getQualifier()
    {
        return qualifier;
    }

    @JsonProperty
    public Domain getDomain()
    {
        return domain;
    }

    @JsonSetter
    public void setDomain(Domain domain)
    {
        this.domain = domain;
    }

    public String toString()
    {
        return toStringHelper(this).add("name", this.name).add("family", this.family).add("qualifier", this.qualifier).add("domain", this.domain).toString();
    }
}
