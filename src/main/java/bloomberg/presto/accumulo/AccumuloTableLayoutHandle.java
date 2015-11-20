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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class AccumuloTableLayoutHandle
    implements ConnectorTableLayoutHandle
{
    private final AccumuloTableHandle table;

    @JsonCreator
    public AccumuloTableLayoutHandle(@JsonProperty("table") AccumuloTableHandle table)
    {
        this.table = table;
    }

    @JsonProperty
    public AccumuloTableHandle getTable()
    {
        return table;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AccumuloTableLayoutHandle that = (AccumuloTableLayoutHandle) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
