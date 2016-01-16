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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import bloomberg.presto.accumulo.serializers.StringRowSerializer;

public final class AccumuloTableProperties {

    private static final String COLUMN_MAPPING = "column_mapping";
    private static final String INTERNAL = "internal";
    private static final String INDEX_COLUMNS = "index_columns";
    private static final String METADATA_ONLY = "metadata_only";
    private static final String SERIALIZER = "serializer";
    private static final String ROW_ID = "row_id";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public AccumuloTableProperties(AccumuloConfig config) {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<String>(COLUMN_MAPPING,
                        "Comma-delimited list of column metadata: col_name:col_family:col_qualifier,[...]",
                        VarcharType.VARCHAR, String.class, null, false,
                        value -> ((String) value).toLowerCase()),
                new PropertyMetadata<String>(INDEX_COLUMNS,
                        "A comma-delimited list of Presto columns that are indexed in this table's corresponding index table.  Default is no indexed columns.",
                        VarcharType.VARCHAR, String.class, "", false,
                        value -> ((String) value).toLowerCase()),
                new PropertyMetadata<Boolean>(INTERNAL,
                        "If true, a DROP TABLE statement WILL delete the corresponding Accumulo table.  Default false.",
                        BooleanType.BOOLEAN, Boolean.class, false, false),
                new PropertyMetadata<Boolean>(METADATA_ONLY,
                        "True to only create metadata about the Accumulo table vs. actually creating the table",
                        BooleanType.BOOLEAN, Boolean.class, false, false),
                new PropertyMetadata<String>(ROW_ID,
                        "Set the column name of the Accumulo row ID.  Default is the first column",
                        VarcharType.VARCHAR, String.class, null, false,
                        value -> ((String) value).toLowerCase()),
                new PropertyMetadata<String>(SERIALIZER,
                        "Serializer for Accumulo data encodings.  Can either be 'default', 'string', 'lexicoder', or a Java class name.  Default is 'default', i.e. the value from AccumuloRowSerializer.getDefault()",
                        VarcharType.VARCHAR, String.class,
                        AccumuloRowSerializer.getDefault().getClass().getName(),
                        false,
                        x -> x.equals("default")
                                ? AccumuloRowSerializer.getDefault().getClass()
                                        .getName()
                                : (x.equals("string")
                                        ? StringRowSerializer.class.getName()
                                        : (x.equals("lexicoder")
                                                ? LexicoderRowSerializer.class
                                                        .getName()
                                                : (String) x))));

    }

    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }

    public static String getColumnMapping(Map<String, Object> tableProperties) {
        return (String) tableProperties.get(COLUMN_MAPPING);
    }

    public static List<String> getIndexColumns(
            Map<String, Object> tableProperties) {
        return Arrays.asList(StringUtils
                .split((String) tableProperties.get(INDEX_COLUMNS), ','));
    }

    public static String getSerializerClass(
            Map<String, Object> tableProperties) {
        return (String) tableProperties.get(SERIALIZER);
    }

    public static String getRowId(Map<String, Object> tableProperties) {
        return (String) tableProperties.get(ROW_ID);
    }

    public static boolean isInternal(Map<String, Object> tableProperties) {
        return (Boolean) tableProperties.get(INTERNAL);
    }

    public static boolean isMetadataOnly(Map<String, Object> tableProperties) {
        return (Boolean) tableProperties.get(METADATA_ONLY);
    }
}
