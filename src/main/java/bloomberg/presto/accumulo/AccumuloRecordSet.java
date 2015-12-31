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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import io.airlift.log.Logger;

public class AccumuloRecordSet implements RecordSet {
    private static final Logger LOG = Logger.get(AccumuloRecordSet.class);
    private final List<AccumuloColumnHandle> columnHandles;
    private final List<AccumuloColumnConstraint> constraints;
    private final List<Type> columnTypes;
    private final Scanner scan;
    private final AccumuloRowSerializer serializer;
    private final ConnectorSession session;

    public AccumuloRecordSet(ConnectorSession session, AccumuloConfig config,
            AccumuloSplit split, List<AccumuloColumnHandle> columnHandles,
            Connector conn) {
        this.session = requireNonNull(session, "session is null");
        requireNonNull(config, "config is null");
        requireNonNull(split, "split is null");
        constraints = requireNonNull(split.getConstraints(),
                "constraints is null");

        try {
            this.serializer = split.getSerializerClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to factory serializer class", e);
        }

        this.columnHandles = requireNonNull(columnHandles,
                "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (AccumuloColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();

        try {
            scan = conn.createScanner(
                    (split.getSchemaName().equals("default") ? ""
                            : split.getSchemaName() + ".")
                            + split.getTableName(),
                    conn.securityOperations()
                            .getUserAuthorizations(config.getUsername()));

            LOG.debug(String.format("Adding range %s",
                    split.getRangeHandle().getRange()));
            scan.setRange(split.getRangeHandle().getRange());
        } catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new AccumuloRecordCursor(session, serializer, scan,
                columnHandles, constraints);
    }
}
