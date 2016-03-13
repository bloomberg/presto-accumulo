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

import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.conf.AccumuloSessionProperties;
import bloomberg.presto.accumulo.conf.AccumuloTableProperties;
import bloomberg.presto.accumulo.index.IndexLookup;
import bloomberg.presto.accumulo.index.Indexer;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.metadata.AccumuloTable;
import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.model.TabletSplitMetadata;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class is the main access point for the Presto connector to interact with Accumulo. It is
 * responsible for creating tables, dropping tables, retrieving table metadata, and getting the
 * ConnectorSplits from a table.
 */
public class AccumuloClient
{
    private static final Logger LOG = Logger.get(AccumuloClient.class);
    private final AccumuloConfig conf;
    private final AccumuloMetadataManager metaManager;
    private final Authorizations auths;
    private final Connector conn;
    private final ZooKeeperInstance inst;
    private final IndexLookup sIndexLookup;

    /**
     * Creates a new instance of an AccumuloClient, injected by that Guice. Creates a connection to
     * Accumulo.
     *
     * @param connectorId
     *            Connector ID
     * @param config
     *            Connector configuration for Accumulo
     * @throws AccumuloException
     *             If an Accumulo error occurs
     * @throws AccumuloSecurityException
     *             If Accumulo credentials are not valid
     */
    @Inject
    public AccumuloClient(AccumuloConnectorId connectorId, AccumuloConfig config)
            throws AccumuloException, AccumuloSecurityException
    {
        this.conf = requireNonNull(config, "config is null");
        this.inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        this.conn = inst.getConnector(config.getUsername(),
                new PasswordToken(config.getPassword().getBytes()));
        this.metaManager = config.getMetadataManager();
        this.auths = conn.securityOperations().getUserAuthorizations(conf.getUsername());
        this.sIndexLookup = new IndexLookup(conn, conf, auths);
    }

    /**
     * Creates a new AccumuloTable based on the given metadata.
     *
     * @param meta
     *            Metadata for the table
     * @return {@link AccumuloTable} for the newly created table
     */
    public AccumuloTable createTable(ConnectorTableMetadata meta)
    {
        // Get the table properties for this table
        Map<String, Object> tableProperties = meta.getProperties();

        // Get the row ID from the properties
        // If null, use the first column
        String rowIdColumn = AccumuloTableProperties.getRowId(tableProperties);
        if (rowIdColumn == null) {
            rowIdColumn = meta.getColumns().get(0).getName();
        }

        // For those who like capital letters
        rowIdColumn = rowIdColumn.toLowerCase();

        // Here, we make sure the user has specified at least one non-row ID column
        // Accumulo requires a column family and qualifier against a row ID, and these values
        // are specified by the other columns in the table
        if (meta.getColumns().size() == 1) {
            throw new InvalidParameterException("Must have at least one non-row ID column");
        }

        // Check all the column types, and throw an exception if the types of a map are complex
        // While it is a rare case, this is not supported by the Accumulo connector
        for (ColumnMetadata column : meta.getColumns()) {
            if (Types.isMapType(column.getType())) {
                if (Types.isMapType(Types.getKeyType(column.getType()))
                        || Types.isMapType(Types.getValueType(column.getType()))
                        || Types.isArrayType(Types.getKeyType(column.getType()))
                        || Types.isArrayType(Types.getValueType(column.getType()))) {
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                            "Key/value types of a map pairs must be plain types");
                }
            }
        }

        // Get the column mappings
        // TODO This kind of sucks from a user perspective, can we use comments instead?
        // COMMENT syntax is not merged into master, see PR 4296
        // https://github.com/facebook/presto/pull/4296
        Map<String, Pair<String, String>> mapping =
                AccumuloTableProperties.getColumnMapping(meta.getProperties());

        // The list of indexed columns
        List<String> indexedColumns = AccumuloTableProperties.getIndexColumns(tableProperties);

        // And now we parse the configured columns and create handles for the
        // metadata manager
        List<AccumuloColumnHandle> columns = new ArrayList<>();
        for (int ordinal = 0; ordinal < meta.getColumns().size(); ++ordinal) {
            ColumnMetadata cm = meta.getColumns().get(ordinal);

            // Special case if this column is the
            if (cm.getName().toLowerCase().equals(rowIdColumn)) {
                columns.add(new AccumuloColumnHandle("accumulo", rowIdColumn, null, null,
                        cm.getType(), ordinal, "Accumulo row ID", false));
            }
            else {
                if (!mapping.containsKey(cm.getName())) {
                    throw new InvalidParameterException(String
                            .format("Misconfigured mapping for presto column %s", cm.getName()));
                }

                // Get the mapping for this column
                Pair<String, String> famqual = mapping.get(cm.getName());
                boolean indexed = indexedColumns.contains(cm.getName().toLowerCase());
                String comment = String.format("Accumulo column %s:%s. Indexed: %b",
                        famqual.getLeft(), famqual.getRight(), indexed);

                // Create a new AccumuloColumnHandle object
                columns.add(new AccumuloColumnHandle("accumulo", cm.getName(), famqual.getLeft(),
                        famqual.getRight(), cm.getType(), ordinal, comment, indexed));
            }
        }

        // Finally create the AccumuloTable object
        boolean external = AccumuloTableProperties.isExternal(tableProperties);
        AccumuloTable table = new AccumuloTable(meta.getTable().getSchemaName(),
                meta.getTable().getTableName(), columns, rowIdColumn, external,
                AccumuloTableProperties.getSerializerClass(tableProperties),
                AccumuloTableProperties.getScanAuthorizations(tableProperties));

        // Validate the Accumulo table exists if it is external
        if (external) {
            boolean tableExists = false;
            boolean indexTableExists = false;
            boolean indexMetricsTableExists = false;
            try {
                tableExists = conn.tableOperations().exists(table.getFullTableName());
                indexTableExists = conn.tableOperations().exists(table.getIndexTableName());
                indexMetricsTableExists =
                        conn.tableOperations().exists(table.getMetricsTableName());
            }
            catch (Exception e) {
                dropTable(table);
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "Accumulo error when validating external tables", e);
            }

            if (!tableExists) {
                throw new PrestoException(StandardErrorCode.USER_ERROR,
                        "Cannot create external table w/o an Accumulo table. Create the "
                                + "Accumulo table first.");
            }

            if (table.isIndexed() && (!indexTableExists || !indexMetricsTableExists)) {
                throw new PrestoException(StandardErrorCode.USER_ERROR,
                        "External table is indexed but the index table and/or index metrics table "
                                + "do not exist.  Create these tables as well and configure the "
                                + "correct iterators and locality groups. See the README");
            }
        }

        // Okay, now we can actually create the metadata and tables. Promise.
        metaManager.createTableMetadata(table);

        // If this table is external, then we are done here
        if (external) {
            return table;
        }

        // But wait! It is internal!
        try {
            // If the table schema is not "default" and the namespace does not exist, create it
            if (!table.getSchema().equals("default")
                    && !conn.namespaceOperations().exists(table.getSchema())) {
                conn.namespaceOperations().create(table.getSchema());
            }

            // Create the table!
            conn.tableOperations().create(table.getFullTableName());

            Map<String, Set<Text>> groups =
                    AccumuloTableProperties.getLocalityGroups(tableProperties);
            // Set locality groups, if any
            if (groups != null && groups.size() > 0) {
                conn.tableOperations().setLocalityGroups(table.getFullTableName(), groups);
                LOG.debug("Set locality groups to %s", groups);
            }
            else {
                LOG.debug("No locality groups set");
            }

            // Create index tables, if appropriate
            createIndexTables(table);
            return table;
        }
        catch (Exception e) {
            dropTable(table);
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Accumulo error when creating table, check coordinator logs for more detail",
                    e);
        }
    }

    /**
     * Creates the index tables from the given Accumulo table. No op if
     * {@link AccumuloTable#isIndexed()} is false.
     *
     * @param table
     *            Table to create index tables
     */
    private void createIndexTables(AccumuloTable table)
    {
        try {
            // If our table is indexed, create the index as well
            if (table.isIndexed()) {
                // Create index table and set the locality groups
                Map<String, Set<Text>> indexGroups = Indexer.getLocalityGroups(table);
                conn.tableOperations().create(table.getIndexTableName());
                conn.tableOperations().setLocalityGroups(table.getIndexTableName(), indexGroups);

                // Create index metrics table, attach iterators, and set locality groups
                conn.tableOperations().create(table.getMetricsTableName());
                conn.tableOperations().setLocalityGroups(table.getMetricsTableName(), indexGroups);
                for (IteratorSetting s : Indexer.getMetricIterators(table)) {
                    conn.tableOperations().attachIterator(table.getMetricsTableName(), s);
                }
            }
        }
        catch (Exception e) {
            dropTable(table);
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Accumulo error when creating index tables, check coordinator logs for more detail",
                    e);
        }
    }

    /**
     * Drops the table metadata from Presto. If this table is internal, the Accumulo
     * tables are also deleted.
     *
     * @param table
     *            The Accumulo table
     */
    public void dropTable(AccumuloTable table)
    {
        SchemaTableName stName = new SchemaTableName(table.getSchema(), table.getTable());
        String tn = table.getFullTableName();

        // Remove the table metadata from Presto
        metaManager.deleteTableMetadata(stName);

        // If the table is external, we won't clean up the Accumulo tables
        if (table.isExternal()) {
            return;
        }

        try {
            // delete the table
            conn.tableOperations().delete(tn);

            // and delete our index tables
            if (table.isIndexed()) {
                conn.tableOperations().delete(Indexer.getIndexTableName(stName));
                conn.tableOperations().delete(Indexer.getMetricsTableName(stName));
            }
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to delete table",
                    e);
        }
    }

    /**
     * Renames the metadata associated Accumulo table
     *
     * @param oldName
     *            Old table name
     * @param newName
     *            New table name
     */
    public void renameTable(SchemaTableName oldName, SchemaTableName newName)
    {
        if (this.getTable(newName) != null) {
            throw new PrestoException(StandardErrorCode.ALREADY_EXISTS,
                    "Table " + newName + " already exists");
        }

        if (!oldName.getSchemaName().equals(newName.getSchemaName())) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                    "Accumulo does not support renaming tables to different namespaces (schemas)");
        }

        AccumuloTable oldTable = getTable(oldName);
        AccumuloTable newTable = new AccumuloTable(newName.getSchemaName(), newName.getTableName(),
                oldTable.getColumns(), oldTable.getRowId(), oldTable.isExternal(),
                oldTable.getSerializerClassName(), oldTable.getScanAuthorizations());

        // Probably being over-cautious on the failure conditions and rollbacks, but I suppose it is
        // better to be safe than sink ships.

        try {
            // First, rename the Accumulo table in the event there is some issue
            conn.tableOperations().rename(oldTable.getFullTableName(), newTable.getFullTableName());
        }
        catch (AccumuloSecurityException | TableNotFoundException | AccumuloException
                | TableExistsException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to rename table",
                    e);
        }

        // Rename index tables as well, if indexed, of course
        if (oldTable.isIndexed()) {
            try {
                conn.tableOperations().rename(oldTable.getIndexTableName(),
                        newTable.getIndexTableName());
            }
            catch (AccumuloSecurityException | TableNotFoundException | AccumuloException
                    | TableExistsException e) {
                try {
                    conn.tableOperations().rename(newTable.getFullTableName(),
                            oldTable.getFullTableName());
                }
                catch (Exception e1) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                            "Failed to rollback table rename", e1);
                }

                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "Failed to rename index table", e);
            }

            try {
                conn.tableOperations().rename(oldTable.getMetricsTableName(),
                        newTable.getMetricsTableName());
            }
            catch (AccumuloSecurityException | TableNotFoundException | AccumuloException
                    | TableExistsException e) {
                try {
                    conn.tableOperations().rename(newTable.getFullTableName(),
                            oldTable.getFullTableName());
                    conn.tableOperations().rename(newTable.getIndexTableName(),
                            oldTable.getIndexTableName());
                }
                catch (Exception e1) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                            "Failed to rollback table rename", e1);
                }

                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "Failed to rename index table", e);
            }
        }

        try {
            // We'll then create the metadata
            metaManager.createTableMetadata(newTable);
        }
        catch (Exception e) {
            // catch all to rollback the operation -- rename the table back
            try {
                conn.tableOperations().rename(newTable.getFullTableName(),
                        oldTable.getFullTableName());

                if (oldTable.isIndexed()) {
                    conn.tableOperations().rename(newTable.getIndexTableName(),
                            oldTable.getIndexTableName());
                    conn.tableOperations().rename(newTable.getMetricsTableName(),
                            oldTable.getMetricsTableName());
                }
            }
            catch (Exception e1) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "Failed to rename table", e1);
            }

            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to create metadata",
                    e);
        }

        try {
            // We'll then delete the metadata
            metaManager.deleteTableMetadata(oldName);
        }
        catch (Exception e) {
            try {
                // catch all to rollback the operation -- rename the table back and create the old
                // metadata
                conn.tableOperations().rename(newTable.getFullTableName(),
                        oldTable.getFullTableName());

                if (oldTable.isIndexed()) {
                    conn.tableOperations().rename(newTable.getIndexTableName(),
                            oldTable.getIndexTableName());
                    conn.tableOperations().rename(newTable.getMetricsTableName(),
                            oldTable.getMetricsTableName());
                }

                metaManager.createTableMetadata(oldTable);
            }
            catch (Exception e1) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "Failed to rollback rename table operation ", e1);
            }

            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to delete metadata",
                    e);
        }
    }

    /**
     * Adds a new column to an existing table
     *
     * @param table
     *            Accumulo table to act on
     * @param column
     *            New column to add
     * @throws IndexOutOfBoundsException
     *             If the ordinal is greater than or equal to the number of columns
     */
    public void addColumn(AccumuloTable table, AccumuloColumnHandle column)
            throws IndexOutOfBoundsException
    {
        // Add the column
        table.addColumn(column);

        // Recreate the table metadata with the new name and exit
        metaManager.deleteTableMetadata(new SchemaTableName(table.getSchema(), table.getTable()));
        metaManager.createTableMetadata(table);

        // Validate index tables exist -- User may have added an index column to a previously
        // un-indexed table
        if (table.isIndexed() && !table.isExternal()) {
            if (!conn.tableOperations().exists(table.getIndexTableName())) {
                createIndexTables(table);
            }
        }
    }

    /**
     * Rename the column of an existing table
     *
     * @param table
     *            Accumulo table to act on
     * @param source
     *            Existing column name
     * @param target
     *            New column name
     */
    public void renameColumn(AccumuloTable table, String source, String target)
    {
        boolean found = false;
        // Locate the column to rename
        for (AccumuloColumnHandle col : table.getColumns()) {
            if (col.getName().equals(source)) {
                found = true;

                // Rename the column
                col.setName(target);

                // Recreate the table metadata with the new name and exit
                metaManager.deleteTableMetadata(
                        new SchemaTableName(table.getSchema(), table.getTable()));
                metaManager.createTableMetadata(table);
                break;
            }
        }

        if (!found) {
            throw new PrestoException(StandardErrorCode.USER_ERROR,
                    format("Failed to find source column %s to rename to %s", source, target));
        }
    }

    /**
     * Gets all schema names via the {@link AccumuloMetadataManager}
     *
     * @return The set of schema names
     */
    public Set<String> getSchemaNames()
    {
        return metaManager.getSchemaNames();
    }

    /**
     * Gets all table names from the given schema
     *
     * @param schema
     *            The schema to get table names from
     * @return The set of table names
     */
    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return metaManager.getTableNames(schema);
    }

    /**
     * Gets the {@link AccumuloTable} for the given name via the {@link AccumuloMetadataManager}
     *
     * @param table
     *            The table to fetch
     * @return The AccumuloTable or null if it does not exist
     */
    public AccumuloTable getTable(SchemaTableName table)
    {
        requireNonNull(table, "schema table name is null");
        return metaManager.getTable(table);
    }

    /**
     * Fetches the TabletSplitMetadata for a query against an Accumulo table. Does a whole bunch of
     * fun stuff! Such as splitting on row ID ranges, applying secondary indexes, column pruning,
     * all sorts of sweet optimizations. What you have here is an important method.
     *
     * @param session
     *            Current session
     * @param schema
     *            Schema name
     * @param table
     *            Table Name
     * @param rowIdDom
     *            Domain for the row ID
     * @param constraints
     *            Column constraints for the query
     * @param serializer
     *            Instance of a row serializer
     * @return List of TabletSplitMetadata objects for Presto
     */
    public List<TabletSplitMetadata> getTabletSplits(ConnectorSession session, String schema,
            String table, Domain rowIdDom, List<AccumuloColumnConstraint> constraints,
            AccumuloRowSerializer serializer)
    {
        try {
            String tableName = AccumuloTable.getFullTableName(schema, table);
            LOG.debug("Getting tablet splits for table %s", tableName);

            // Get the initial Range based on the row ID domain
            final Collection<Range> rowIdRanges = getRangesFromDomain(rowIdDom, serializer);
            final List<TabletSplitMetadata> tabletSplits = new ArrayList<>();

            // Check the secondary index based on the column constraints
            // If this returns true, return the tablet splits to Presto
            if (sIndexLookup.applyIndex(schema, table, session, constraints, rowIdRanges,
                    tabletSplits, serializer)) {
                return tabletSplits;
            }

            // If we can't (or shouldn't) use the secondary index,
            // we will just use the Range from the row ID domain

            // Split the ranges on tablet boundaries, if enabled
            final Collection<Range> splitRanges;
            if (AccumuloSessionProperties.isOptimizeSplitRangesEnabled(session)) {
                splitRanges = splitByTabletBoundaries(tableName, rowIdRanges);
            }
            else {
                // if not enabled, just use the same collection
                splitRanges = rowIdRanges;
            }

            // Create TabletSplitMetadata objects for each range
            boolean fetchTabletLocations =
                    AccumuloSessionProperties.isOptimizeLocalityEnabled(session);
            String defaultLocation = "localhost:9997";
            for (Range r : splitRanges) {
                // If locality is enabled, then fetch tablet location
                if (fetchTabletLocations) {
                    tabletSplits.add(new TabletSplitMetadata(
                            getTabletLocation(tableName, r.getStartKey()), ImmutableList.of(r)));
                }
                else {
                    // else, just use the default location
                    tabletSplits.add(new TabletSplitMetadata(defaultLocation, ImmutableList.of(r)));
                }
            }

            // Log some fun stuff and return the tablet splits
            LOG.debug("Number of splits for table %s is %d with %d ranges", tableName,
                    tabletSplits.size(), splitRanges.size());
            return tabletSplits;
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to get splits", e);
        }
    }

    /**
     * Splits the given collection of ranges based on tablet boundaries, returning a new collection
     * of ranges
     *
     * @param tableName
     *            Fully-qualified table name
     * @param ranges
     *            The collection of Ranges to split
     * @return A new collection of Ranges split on tablet boundaries
     * @throws TableNotFoundException
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    private Collection<Range> splitByTabletBoundaries(String tableName, Collection<Range> ranges)
            throws TableNotFoundException, AccumuloException, AccumuloSecurityException
    {
        Collection<Range> splitRanges = new HashSet<>();
        for (Range r : ranges) {
            // if start and end key are equivalent, no need to split the range
            if (r.getStartKey() != null && r.getEndKey() != null
                    && r.getStartKey().equals(r.getEndKey())) {
                splitRanges.add(r);
            }
            else {
                // Call out to Accumulo to split the range on tablets
                splitRanges.addAll(conn.tableOperations().splitRangeByTablets(tableName, r,
                        Integer.MAX_VALUE));
            }
        }
        return splitRanges;
    }

    /**
     * Gets the TabletServer hostname for where the given key is located in the given table
     *
     * @param table
     *            Fully-qualified table name
     * @param key
     *            Key to locate
     * @return The table location, or the default location via
     *         {@link AccumuloClient#getDefaultTabletLocation(String)}
     */
    private String getTabletLocation(String table, Key key)
    {
        try {
            // Get the Accumulo table ID so we can scan some fun stuff
            String tableId = conn.tableOperations().tableIdMap().get(table);

            // Create our scanner against the metadata table, fetching 'loc' family
            Scanner scan = conn.createScanner("accumulo.metadata", auths);
            scan.fetchColumnFamily(new Text("loc"));

            // Set the scan range to just this table, from the table ID to the default tablet
            // row, which is the last listed tablet
            Key defaultTabletRow = new Key(tableId + '<');
            Key start = new Key(tableId);
            Key end = defaultTabletRow.followingKey(PartialKey.ROW);
            scan.setRange(new Range(start, end));

            String location = null;
            if (key == null) {
                // if the key is null, then it is -inf, so get first tablet location
                for (Entry<Key, Value> kvp : scan) {
                    location = kvp.getValue().toString();
                    break;
                }
            }
            else {
                // Else, we will need to scan through the tablet location data and find the location

                // Create some text objects to do comparison for what we are looking for
                Text splitCompareKey = new Text();
                key.getRow(splitCompareKey);
                Text scannedCompareKey = new Text();

                // Scan the table!
                for (Entry<Key, Value> kvp : scan) {
                    // Get the bytes of the key
                    byte[] keyBytes = kvp.getKey().getRow().copyBytes();

                    // Chop off some magic nonsense
                    scannedCompareKey.set(keyBytes, 3, keyBytes.length - 3);

                    // Compare the keys, moving along the tablets until the location is found
                    if (scannedCompareKey.getLength() > 0) {
                        int compareTo = splitCompareKey.compareTo(scannedCompareKey);
                        if (compareTo <= 0) {
                            location = kvp.getValue().toString();
                        }
                        else {
                            // all future tablets will be > this key
                            break;
                        }
                    }
                }
                scan.close();
            }

            LOG.info("Key %s location %s", key != null ? key.getRow() : null, location);
            // If we were unable to find the location for some reason, return the default tablet
            // location
            return location != null ? location : getDefaultTabletLocation(table);
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Failed to get tablet location");
        }
    }

    /**
     * Gets the location of the default tablet for the given table
     *
     * @param fulltable
     *            Fully-qualified table name
     * @return TabletServer location of the default tablet
     */
    private String getDefaultTabletLocation(String fulltable)
    {
        try {
            // Get the table ID
            String tableId = conn.tableOperations().tableIdMap().get(fulltable);

            // Create a scanner over the metadata table, fetching the 'loc' column of the default
            // tablet row
            Scanner scan = conn.createScanner("accumulo.metadata",
                    conn.securityOperations().getUserAuthorizations(conf.getUsername()));
            scan.fetchColumnFamily(new Text("loc"));
            scan.setRange(new Range(tableId + '<'));

            // scan the entry
            String location = null;
            for (Entry<Key, Value> kvp : scan) {
                if (location != null) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                            "Scan for default tablet returned more than one entry");
                }

                location = kvp.getValue().toString();
            }

            scan.close();
            return location;
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Failed to get tablet location");
        }
    }

    /**
     * Gets a collection of Accumulo Range objects from the given Presto domain. This maps the
     * column constraints of the given Domain to an Accumulo Range scan.
     *
     * @param dom
     *            Domain, can be null (returns (-inf, +inf) Range)
     * @param serializer
     *            Instance of an {@link AccumuloRowSerializer}
     * @return A collection of Accumulo Range objects
     * @throws AccumuloException
     *             If an Accumulo error occurs
     * @throws AccumuloSecurityException
     *             If Accumulo credentials are not valid
     * @throws TableNotFoundException
     *             If the Accumulo table is not found
     */
    public static Collection<Range> getRangesFromDomain(Domain dom,
            AccumuloRowSerializer serializer)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        // if we have no predicate pushdown, use the full range
        if (dom == null) {
            return ImmutableSet.of(new Range());
        }

        Set<Range> ranges = new HashSet<>();

        // If this domain is from an ANY clause
        if (dom.getValues().isAny()) {
            // Get the domain type
            Type t = dom.getType();
            if (Types.isArrayType(t)) {
                // If it is an array type, then each value is in the domain is an array
                for (Object arrayBlock : dom.getValues().getDiscreteValues().getValues()) {
                    // Get the elements of the array block and add them to the ranges
                    Type elementType = Types.getElementType(t);
                    for (Object o : AccumuloRowSerializer.getArrayFromBlock(elementType,
                            (Block) arrayBlock)) {
                        // Find all locations like this and work on it
                        ranges.add(new Range(new Text(serializer.encode(elementType, o))));
                    }
                }
            }
            else {
                // If it is not an array type, then this is just a list of values
                for (Object o : dom.getValues().getDiscreteValues().getValues()) {
                    ranges.add(new Range(new Text(serializer.encode(t, o))));
                }
            }
        }
        else {
            // This isn't an ANY clause, so convert the Presto Range to an Accumulo Range
            for (com.facebook.presto.spi.predicate.Range r : dom.getValues().getRanges()
                    .getOrderedRanges()) {
                ranges.add(getRangeFromPrestoRange(r, serializer));
            }
        }
        return ranges;
    }

    /**
     * Convert the given Presto range to an Accumulo range
     *
     * @param pRange
     *            Presto range
     * @param serializer
     *            Instance of an {@link AccumuloRowSerializer}
     * @return Accumulo range
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableNotFoundException
     */
    private static Range getRangeFromPrestoRange(com.facebook.presto.spi.predicate.Range pRange,
            AccumuloRowSerializer serializer)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        final Range aRange;
        if (pRange.isAll()) {
            aRange = new Range();
        }
        else if (pRange.isSingleValue()) {
            Text split = new Text(serializer.encode(pRange.getType(), pRange.getSingleValue()));
            aRange = new Range(split);
        }
        else {
            if (pRange.getLow().isLowerUnbounded()) {
                // If low is unbounded, then create a range from (-inf, value), checking
                // inclusivity
                boolean inclusive = pRange.getHigh().getBound() == Bound.EXACTLY;
                Text split =
                        new Text(serializer.encode(pRange.getType(), pRange.getHigh().getValue()));
                aRange = new Range(null, false, split, inclusive);
            }
            else if (pRange.getHigh().isUpperUnbounded()) {
                // If high is unbounded, then create a range from (value, +inf), checking
                // inclusivity
                boolean inclusive = pRange.getLow().getBound() == Bound.EXACTLY;
                Text split =
                        new Text(serializer.encode(pRange.getType(), pRange.getLow().getValue()));
                aRange = new Range(split, inclusive, null, false);
            }
            else {
                // If high is unbounded, then create a range from low to high, checking
                // inclusivity
                boolean startKeyInclusive = pRange.getLow().getBound() == Bound.EXACTLY;
                Text startSplit =
                        new Text(serializer.encode(pRange.getType(), pRange.getLow().getValue()));

                boolean endKeyInclusive = pRange.getHigh().getBound() == Bound.EXACTLY;
                Text endSplit =
                        new Text(serializer.encode(pRange.getType(), pRange.getHigh().getValue()));
                aRange = new Range(startSplit, startKeyInclusive, endSplit, endKeyInclusive);
            }
        }

        return aRange;
    }
}
