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
    private Domain domain;

    @JsonCreator
    public AccumuloColumnConstraint(@JsonProperty("name") String name,
            @JsonProperty("domain") Domain domain) {
        this.name = requireNonNull(name, "name is null");
        this.domain = domain;
        Logger.get(getClass()).debug("CONST DOMAIN IS " + domain);
    }

    @JsonProperty
    public String getName() {
        return name;
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

    @Override
    public String toString() {
        return toStringHelper(this).add("name", name).add("domain", domain)
                .toString();
    }
}
