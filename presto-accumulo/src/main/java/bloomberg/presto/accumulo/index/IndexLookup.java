package bloomberg.presto.accumulo.index;

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.AccumuloSessionProperties;
import bloomberg.presto.accumulo.RangeHandle;
import bloomberg.presto.accumulo.TabletSplitMetadata;
import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class IndexLookup
{
    private static final Logger LOG = Logger.get(IndexLookup.class);
    private static final Range METRICS_TABLE_ROWID_RANGE = new Range(Indexer.METRICS_TABLE_ROWID_AS_TEXT);

    private final ColumnCardinalityCache ccCache;
    private final Connector conn;
    private final Authorizations auths;
    private final Text tmpCQ = new Text();
    private final Map<AccumuloColumnConstraint, Collection<Range>> constraintRangePairs = new HashMap<>();
    private final ConstraintComparator constraintComparator = new ConstraintComparator();

    public IndexLookup(Connector conn, AccumuloConfig config, Authorizations auths)
            throws AccumuloException, AccumuloSecurityException
    {
        this.conn = conn;
        this.auths = auths;
        this.ccCache = new ColumnCardinalityCache(conn, config, auths);
    }

    public boolean applySecondaryIndex(String schema, String table, ConnectorSession session, Collection<AccumuloColumnConstraint> constraints, Collection<Range> rowIdRanges, List<TabletSplitMetadata> tabletSplits)
            throws Exception
    {
        if (!AccumuloSessionProperties.isSecondaryIndexEnabled(session)) {
            LOG.debug("Secondary index is disabled");
            return false;
        }

        LOG.debug("Secondary index is enabled");

        constraintRangePairs.clear();
        // Collect Accumulo ranges for each indexd column constraint
        for (AccumuloColumnConstraint acc : constraints) {
            if (acc.isIndexed()) {
                constraintRangePairs.put(acc, AccumuloClient.getRangesFromDomain(acc.getDomain()));
            }
            else {
                LOG.warn("Query containts constraint on non-indexed column %s", acc.getName());
            }
        }

        if (constraintRangePairs.size() == 0) {
            LOG.debug("Query contains no constraints on indexed columns, skipping secondary index");
            return false;
        }

        // get the cardinalities from the metrics table
        List<Pair<AccumuloColumnConstraint, Long>> cardinalities = ccCache.getCardinalities(schema, table, constraintRangePairs);

        // order by cardinality, ascending
        Collections.sort(cardinalities, constraintComparator);

        // if first entry has cardinality zero
        if (cardinalities.get(0).getRight() == 0) {
            LOG.debug("Query would return no results, returning empty list of splits");
            return true;
        }

        String indexTable = Indexer.getIndexTableName(schema, table);
        String metricsTable = Indexer.getMetricsTableName(schema, table);
        long numRows = getNumRowsInTable(metricsTable);
        double threshold = AccumuloSessionProperties.getIndexRatio(session);
        final Collection<Range> idxRanges;
        // if we should use the intersection of the columns
        if (smallestCardAboveThreshold(session, numRows, cardinalities)) {
            // if we only have one column, we can skip the intersection process
            if (cardinalities.size() == 1) {
                long numEntries = cardinalities.get(0).getRight();
                double ratio = ((double) numEntries / (double) numRows);
                LOG.debug("Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for table? %b", numEntries, numRows, ratio, threshold, ratio < threshold, table);
                return false;
            }

            // compute the intersection of the ranges prior to checking the threshold
            LOG.debug("%d indexed columns, intersecting ranges", constraintRangePairs.size());
            idxRanges = getIndexRanges(indexTable, constraintRangePairs, rowIdRanges);
            LOG.debug("Intersection results in %d ranges from secondary index", idxRanges.size());
        }
        else {
            LOG.debug("Not intersecting columns, using column with lowest cardinality ");
            idxRanges = getIndexRanges(indexTable, ImmutableMap.of(cardinalities.get(0).getKey(), constraintRangePairs.get(cardinalities.get(0).getKey())), rowIdRanges);
        }

        long numEntries = idxRanges.size();
        double ratio = (double) numEntries / (double) numRows;
        LOG.debug("Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for table? %b", numEntries, numRows, ratio, threshold, ratio < threshold, table);

        if (ratio < threshold) {
            binRanges(AccumuloSessionProperties.getSecondaryIndexRangesPerSplit(session), idxRanges, tabletSplits);
            LOG.debug("Number of splits for %s.%s is %d with %d ranges", schema, table, tabletSplits.size(), idxRanges.size());
            return true;
        }
        else {
            return false;
        }
    }

    private boolean smallestCardAboveThreshold(ConnectorSession session, long numRows, List<Pair<AccumuloColumnConstraint, Long>> cardinalities)
    {
        long lowCard = cardinalities.get(0).getRight();
        double ratio = ((double) lowCard / (double) numRows);
        double threshold = AccumuloSessionProperties.getLowestCardinalityThreshold(session);
        LOG.debug("Lowest cardinality is %d, num rows is %d, ratio is %2f with threshold of %f", lowCard, numRows, ratio, threshold);
        return ratio > threshold;
    }

    private long getNumRowsInTable(String metricsTable)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        Scanner scan = conn.createScanner(metricsTable, auths);
        scan.fetchColumn(Indexer.METRICS_TABLE_ROWS_CF_AS_TEXT, Indexer.CARDINALITY_CQ_AS_TEXT);
        scan.setRange(METRICS_TABLE_ROWID_RANGE);

        long numRows = -1;
        for (Entry<Key, Value> entry : scan) {
            if (numRows > 0) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Should have received only one entry");
            }
            numRows = Long.parseLong(entry.getValue().toString());
        }
        scan.close();

        LOG.debug("Number of rows in table is %d", numRows);
        return numRows;
    }

    private Collection<Range> getIndexRanges(String indexTable, Map<AccumuloColumnConstraint, Collection<Range>> constraintRangePairs, Collection<Range> predicatePushdownRanges)
            throws AccumuloException,
            AccumuloSecurityException, TableNotFoundException
    {
        Set<Range> finalRanges = null;
        for (Entry<AccumuloColumnConstraint, Collection<Range>> e : constraintRangePairs.entrySet()) {
            BatchScanner scan = conn.createBatchScanner(indexTable, auths, 10);
            scan.setRanges(e.getValue());
            Text cf = new Text(Indexer.getIndexColumnFamily(e.getKey().getFamily().getBytes(), e.getKey().getQualifier().getBytes()).array());
            scan.fetchColumnFamily(cf);

            Set<Range> columnRanges = new HashSet<>();
            for (Entry<Key, Value> entry : scan) {
                entry.getKey().getColumnQualifier(tmpCQ);

                if (inRange(tmpCQ, predicatePushdownRanges)) {
                    columnRanges.add(new Range(tmpCQ));
                }
            }

            LOG.debug("Retrieved %d ranges for column %s", columnRanges.size(), e.getKey().getName());
            if (finalRanges == null) {
                finalRanges = new HashSet<>();
                finalRanges.addAll(columnRanges);
            }
            else {
                finalRanges.retainAll(columnRanges);
            }

            scan.close();
        }

        return finalRanges;
    }

    private void binRanges(int numRangesPerBin, Collection<Range> splitRanges, List<TabletSplitMetadata> prestoSplits)
    {
        // convert Ranges to RangeHandles
        List<RangeHandle> rHandles = new ArrayList<>();
        splitRanges.stream().forEach(x -> rHandles.add(RangeHandle.from(x)));

        // Bin them together into splits
        Collections.shuffle(rHandles);

        String loc = "localhost:9997";
        int toAdd = rHandles.size();
        int fromIndex = 0;
        int toIndex = Math.min(toAdd, numRangesPerBin);
        do {
            prestoSplits.add(new TabletSplitMetadata(loc, rHandles.subList(fromIndex, toIndex)));
            toAdd -= toIndex - fromIndex;
            fromIndex = toIndex;
            toIndex += Math.min(toAdd, numRangesPerBin);
        }
        while (toAdd > 0);
    }

    private boolean inRange(Text cq, Collection<Range> preSplitRanges)
    {
        Key kCq = new Key(cq);
        for (Range r : preSplitRanges) {
            if (!r.beforeStartKey(kCq) && !r.afterEndKey(kCq)) {
                return true;
            }
        }
        return false;
    }

    public class ConstraintComparator
            implements Comparator<Pair<AccumuloColumnConstraint, Long>>
    {
        @Override
        public int compare(Pair<AccumuloColumnConstraint, Long> o1, Pair<AccumuloColumnConstraint, Long> o2)
        {
            return o1.getRight().compareTo(o2.getRight());
        }
    }
}
