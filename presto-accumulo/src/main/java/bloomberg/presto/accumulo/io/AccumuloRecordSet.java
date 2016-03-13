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
package bloomberg.presto.accumulo.io;

import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.model.AccumuloSplit;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of a Presto RecordSet, responsible for returning the column types and the
 * RecordCursor to the framework.
 *
 * @see AccumuloRecordCursor
 * @see AccumuloRecordSetProvider
 */
public class AccumuloRecordSet
        implements RecordSet
{
    private static final Logger LOG = Logger.get(AccumuloRecordSet.class);

    private final List<AccumuloColumnHandle> columnHandles;
    private final List<AccumuloColumnConstraint> constraints;
    private final List<Type> columnTypes;
    private final AccumuloRowSerializer serializer;
    private final BatchScanner scan;
    private final String rowIdName;

    /**
     * Creates a new instance of {@link AccumuloRecordSet}
     *
     * @param session
     *            Current client session
     * @param config
     *            Connector configuration
     * @param split
     *            Split to process
     * @param columnHandles
     *            Columns of the table
     * @param conn
     *            Accumulo connector
     */
    public AccumuloRecordSet(ConnectorSession session, AccumuloConfig config, AccumuloSplit split,
            List<AccumuloColumnHandle> columnHandles, Connector conn)
    {
        requireNonNull(config, "config is null");
        requireNonNull(split, "split is null");
        constraints = requireNonNull(split.getConstraints(), "constraints is null");

        rowIdName = split.getRowId();

        // Factory the serializer based on the split configuration
        try {
            this.serializer = split.getSerializerClass().newInstance();
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Failed to factory serializer class.  Is it on the classpath?", e);
        }

        // Save off the column handles and createa list of the Accumulo types
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (AccumuloColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();

        try {
            // Get the scan-time authorizations from the connector split, or use the user's
            // authorizations if not set
            final Authorizations auths;
            if (split.hasScanAuthorizations()) {
                auths = new Authorizations(split.getScanAuthorizations().split(","));
                LOG.info("scan_auths set: %s", auths);
            }
            else {
                auths = conn.securityOperations().getUserAuthorizations(config.getUsername());
                LOG.info("scan_auths not set, using user auths: %s", auths);
            }

            // Create the BatchScanner and set the ranges from the split
            scan = conn.createBatchScanner(split.getFullTableName(), auths, 10);
            scan.setRanges(split.getRanges());
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
    }

    /**
     * Gets the types of all the columns in field order
     *
     * @return List of Presto types
     */
    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    /**
     * Gets a new RecordCursor to iterate over rows of data as defined by the RecordSet's split
     *
     * @return RecordCursor
     */
    @Override
    public RecordCursor cursor()
    {
        return new AccumuloRecordCursor(serializer, scan, rowIdName, columnHandles, constraints);
    }
}
