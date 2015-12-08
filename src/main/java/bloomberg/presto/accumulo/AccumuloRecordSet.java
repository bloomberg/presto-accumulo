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

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

public class AccumuloRecordSet implements RecordSet {
    private final List<AccumuloColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Scanner scan;

    public AccumuloRecordSet(AccumuloConfig config, AccumuloSplit split,
            List<AccumuloColumnHandle> columnHandles, Connector conn) {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles,
                "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (AccumuloColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        try {
            scan = conn.createScanner(
                    (split.getSchemaName().equals("default") ? ""
                            : split.getSchemaName() + ".")
                            + split.getTableName(),
                    conn.securityOperations()
                            .getUserAuthorizations(config.getUsername()));
            scan.setRange(new Range());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new AccumuloRecordCursor(columnHandles, scan);
    }
}
