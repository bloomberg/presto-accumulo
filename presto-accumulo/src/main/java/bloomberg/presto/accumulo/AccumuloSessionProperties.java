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

    private static final String OPTIMIZE_COLUMN_FILTERS_ENABLED = "optimize_column_filters_enabled";
    private static final String OPTIMIZE_RANGE_SPLITS_ENABLED = "optimize_range_splits_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public AccumuloSessionProperties(AccumuloConfig config) {
        sessionProperties = ImmutableList.of(
                booleanSessionProperty(OPTIMIZE_COLUMN_FILTERS_ENABLED,
                        "Set to true to enable the column value filter pushdowns.  Default true.",
                        true, false),
                booleanSessionProperty(OPTIMIZE_RANGE_SPLITS_ENABLED,
                        "Set to true to enable splitting the query by tablets.  Typically for testing only.  Default true.",
                        true, false));
    }

    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    public static boolean isOptimizeColumnFiltersEnabled(
            ConnectorSession session) {
        return session.getProperty(OPTIMIZE_COLUMN_FILTERS_ENABLED,
                Boolean.class);
    }

    public static boolean isOptimizeRangeSplitsEnabled(
            ConnectorSession session) {
        return session.getProperty(OPTIMIZE_RANGE_SPLITS_ENABLED,
                Boolean.class);
    }
}
