/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bloomberg.presto.accumulo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class AccumuloColumn {
    private final String name;
    private final String family;
    private final String qualifier;
    private final Type type;

    @JsonCreator
    public AccumuloColumn(@JsonProperty("name") String name,
            @JsonProperty("family") String family,
            @JsonProperty("qualifier") String qualifier,
            @JsonProperty("type") Type type) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        checkArgument(!isNullOrEmpty(family), "family is null or is empty");
        checkArgument(!isNullOrEmpty(qualifier),
                "qualifier is null or is empty");
        this.name = name;
        this.family = family;
        this.qualifier = qualifier;
        this.type = requireNonNull(type, "type is null");
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
    public Type getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(family, qualifier, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        AccumuloColumn other = (AccumuloColumn) obj;
        return Objects.equals(this.family, other.family)
                && Objects.equals(this.qualifier, other.qualifier)
                && Objects.equals(this.type, other.type);
    }

    @Override
    public String toString() {
        return family + ":" + qualifier + ":" + type;
    }
}
