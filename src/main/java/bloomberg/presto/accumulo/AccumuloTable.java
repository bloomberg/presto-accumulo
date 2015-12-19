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

import java.util.List;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class AccumuloTable {
    private final String name;
    private final List<AccumuloColumnHandle> columns;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public AccumuloTable(@JsonProperty("name") String name,
            @JsonProperty("columns") List<AccumuloColumnHandle> columns) {
        checkArgument(!isNullOrEmpty(name), "table name is null or is empty");
        this.name = requireNonNull(name, "table name is null");
        this.columns = ImmutableList
                .copyOf(requireNonNull(columns, "table columns are null"));

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList
                .builder();
        for (AccumuloColumnHandle column : this.columns) {
            columnsMetadata.add(column.getColumnMetadata());
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public List<AccumuloColumnHandle> getColumns() {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }
}
