/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;

public final class AccumuloSessionProperties
{
    private static final String INT_OPTIMIZE_COLUMN_FILTERS_ENABLED = "optimize_column_filters_enabled";
    private static final String INT_OPTIMIZE_LOCALITY_ENABLED = "optimize_locality_enabled";
    private static final String INT_OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED = "optimize_range_predicate_pushdown_enabled";
    private static final String INT_OPTIMIZE_RANGE_SPLITS_ENABLED = "optimize_range_splits_enabled";
    private static final String INT_SECONDARY_INDEX_ENABLED = "secondary_index_enabled";
    private static final String INT_RANGES_PER_SPLIT = "ranges_per_split";
    private static final String INT_SECONDARY_INDEX_THRESHOLD = "secondary_index_threshold";
    private static final String INT_NUM_ARTIFICIAL_SPLITS = "num_artificial_splits";

    public static final String OPTIMIZE_COLUMN_FILTERS_ENABLED = "accumulo." + INT_OPTIMIZE_COLUMN_FILTERS_ENABLED;
    public static final String OPTIMIZE_LOCALITY_ENABLED = "accumulo." + INT_OPTIMIZE_LOCALITY_ENABLED;
    public static final String OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED = "accumulo." + INT_OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED;
    public static final String OPTIMIZE_RANGE_SPLITS_ENABLED = "accumulo." + INT_OPTIMIZE_RANGE_SPLITS_ENABLED;
    public static final String SECONDARY_INDEX_ENABLED = "accumulo." + INT_SECONDARY_INDEX_ENABLED;
    public static final String RANGES_PER_SPLIT = "accumulo." + INT_RANGES_PER_SPLIT;
    public static final String SECONDARY_INDEX_THRESHOLD = "accumulo." + INT_SECONDARY_INDEX_THRESHOLD;
    public static final String NUM_ARTIFICIAL_SPLITS = "accumulo." + INT_NUM_ARTIFICIAL_SPLITS;

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public AccumuloSessionProperties(AccumuloConfig config)
    {
        PropertyMetadata<Boolean> s1 = booleanSessionProperty(INT_OPTIMIZE_COLUMN_FILTERS_ENABLED, "Set to true to enable the column value filter pushdowns.  Default true.", true, false);
        PropertyMetadata<Boolean> s2 = booleanSessionProperty(INT_OPTIMIZE_LOCALITY_ENABLED, "Set to true to enable data locality lookups for scan ranges.  Default true.", true, false);
        PropertyMetadata<Boolean> s3 = booleanSessionProperty(INT_OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED, "Set to true to enable using predicate pushdowns on row ID.  Default true.", true, false);
        PropertyMetadata<Boolean> s4 = booleanSessionProperty(INT_OPTIMIZE_RANGE_SPLITS_ENABLED, "Set to true to enable splitting the query by tablets.  Default true.", true, false);
        PropertyMetadata<Boolean> s5 = booleanSessionProperty(INT_SECONDARY_INDEX_ENABLED, "Set to true to enable usage of the secondary index on query.  Default true.", true, false);
        PropertyMetadata<Integer> s6 = integerSessionProperty(INT_RANGES_PER_SPLIT, "The number of Accumulo ranges that are packed into a single Presto split.  Default 10000", 10000, false);
        PropertyMetadata<Double> s7 = doubleSessionProperty(INT_SECONDARY_INDEX_THRESHOLD, "The ratio between number of rows to be scanned based on the secondary index over the total number of rows.  If the ratio is below this threshold, the secondary index will be used.  Default .2", .2, false);
        PropertyMetadata<Integer> s8 = integerSessionProperty(INT_NUM_ARTIFICIAL_SPLITS, "The power-of-two for the number of artificial splits created for ranges of row IDs. A value of 0 is disabled, 1 is 2^1 splits or two splits per range, 2 is 2^2 or four splits per range, etc.  Default 0 (disabled)", 0, false);

        sessionProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6, s7, s8);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isOptimizeColumnFiltersEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_OPTIMIZE_COLUMN_FILTERS_ENABLED, Boolean.class);
    }

    public static boolean isOptimizeLocalityEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_OPTIMIZE_LOCALITY_ENABLED, Boolean.class);
    }

    public static boolean isOptimizeRangePredicatePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isOptimizeRangeSplitsEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_OPTIMIZE_RANGE_SPLITS_ENABLED, Boolean.class);
    }

    public static boolean isSecondaryIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_SECONDARY_INDEX_ENABLED, Boolean.class);
    }

    public static double getIndexRatio(ConnectorSession session)
    {
        return session.getProperty(INT_SECONDARY_INDEX_THRESHOLD, Double.class);
    }

    public static int getRangesPerSplit(ConnectorSession session)
    {
        return session.getProperty(INT_RANGES_PER_SPLIT, Integer.class);
    }

    public static int getNumArtificialSplits(ConnectorSession session)
    {
        return session.getProperty(INT_NUM_ARTIFICIAL_SPLITS, Integer.class);
    }
}
