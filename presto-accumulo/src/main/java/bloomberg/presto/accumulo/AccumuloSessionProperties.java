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

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;

import java.util.List;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

public final class AccumuloSessionProperties {

    private static final String INT_OPTIMIZE_COLUMN_FILTERS_ENABLED = "optimize_column_filters_enabled";
    private static final String INT_OPTIMIZE_LOCALITY_ENABLED = "optimize_locality_enabled";
    private static final String INT_OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED = "optimize_range_predicate_pushdown_enabled";
    private static final String INT_OPTIMIZE_RANGE_SPLITS_ENABLED = "optimize_range_splits_enabled";
    private static final String INT_SECONDARY_INDEX_ENABLED = "secondary_index_enabled";

    public static final String OPTIMIZE_COLUMN_FILTERS_ENABLED = "accumulo."
            + INT_OPTIMIZE_COLUMN_FILTERS_ENABLED;
    public static final String OPTIMIZE_LOCALITY_ENABLED = "accumulo."
            + INT_OPTIMIZE_LOCALITY_ENABLED;
    public static final String OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED = "accumulo."
            + INT_OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED;
    public static final String OPTIMIZE_RANGE_SPLITS_ENABLED = "accumulo."
            + INT_OPTIMIZE_RANGE_SPLITS_ENABLED;
    public static final String SECONDARY_INDEX_ENABLED = "accumulo."
            + INT_SECONDARY_INDEX_ENABLED;

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public AccumuloSessionProperties(AccumuloConfig config) {
        sessionProperties = ImmutableList.of(
                booleanSessionProperty(INT_OPTIMIZE_COLUMN_FILTERS_ENABLED,
                        "Set to true to enable the column value filter pushdowns.  Default true.",
                        true, false),
                booleanSessionProperty(INT_OPTIMIZE_LOCALITY_ENABLED,
                        "Set to true to enable data locality lookups for scan ranges.  Default true.",
                        true, false),
                booleanSessionProperty(
                        INT_OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED,
                        "Set to true to enable using predicate pushdowns on row ID.  Typically for testing only.  Default true.",
                        true, false),
                booleanSessionProperty(INT_OPTIMIZE_RANGE_SPLITS_ENABLED,
                        "Set to true to enable splitting the query by tablets.  Typically for testing only.  Default true.",
                        true, false),
                booleanSessionProperty(INT_SECONDARY_INDEX_ENABLED,
                        "Set to true to enable usage of the secondary index on query.  Typically for testing only.  Default true.",
                        true, false));
    }

    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    public static boolean isOptimizeColumnFiltersEnabled(
            ConnectorSession session) {
        return session.getProperty(INT_OPTIMIZE_COLUMN_FILTERS_ENABLED,
                Boolean.class);
    }

    public static boolean isOptimizeLocalityEnabled(ConnectorSession session) {
        return session.getProperty(INT_OPTIMIZE_LOCALITY_ENABLED,
                Boolean.class);
    }

    public static boolean isOptimizeRangePredicatePushdownEnabled(
            ConnectorSession session) {
        return session.getProperty(
                INT_OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isOptimizeRangeSplitsEnabled(
            ConnectorSession session) {
        return session.getProperty(INT_OPTIMIZE_RANGE_SPLITS_ENABLED,
                Boolean.class);
    }

    public static boolean isSecondaryIndexEnabled(ConnectorSession session) {
        return session.getProperty(INT_SECONDARY_INDEX_ENABLED, Boolean.class);
    }
}
