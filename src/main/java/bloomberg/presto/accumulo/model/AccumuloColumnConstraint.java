package bloomberg.presto.accumulo.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.facebook.presto.spi.predicate.Domain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import io.airlift.log.Logger;

public class AccumuloColumnConstraint {
    private final String name;
    private final String family;
    private final String qualifier;
    private Domain domain;

    @JsonCreator
    public AccumuloColumnConstraint(@JsonProperty("name") String name,
            @JsonProperty("family") String family,
            @JsonProperty("qualifier") String qualifier,
            @JsonProperty("domain") Domain domain) {
        this.name = requireNonNull(name, "name is null");
        this.family = requireNonNull(family, "family is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        this.domain = domain;
        Logger.get(getClass()).debug("CONST DOMAIN IS " + domain);
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFamily() {
        return family;
    }

    @JsonProperty
    public String getQualifier() {
        return qualifier;
    }

    @JsonProperty
    public Domain getDomain() {
        return domain;
    }

    @JsonSetter
    public void setDomain(Domain domain) {
        this.domain = domain;
        Logger.get(getClass()).debug("SET DOMAIN IS " + domain);
    }

    public String toString() {
        return toStringHelper(this).add("name", this.name)
                .add("family", this.family).add("qualifier", this.qualifier)
                .add("domain", this.domain).toString();
    }
}
