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

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;

import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import bloomberg.presto.accumulo.serializers.StringRowSerializer;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

public class AccumuloConnector implements Connector {
    public static final String PROP_COLUMN_MAPPING = "column_mapping";
    public static final String PROP_INTERNAL = "internal";
    public static final String PROP_METADATA_ONLY = "metadata_only";
    public static final String PROP_SERIALIZER = "serializer";
    private static final Logger LOG = Logger.get(AccumuloConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final AccumuloMetadata metadata;
    private final AccumuloSplitManager splitManager;
    private final AccumuloRecordSetProvider recordSetProvider;
    private final AccumuloHandleResolver handleResolver;
    private final AccumuloPageSinkProvider pageSinkProvider;

    @Inject
    public AccumuloConnector(LifeCycleManager lifeCycleManager,
            AccumuloMetadata metadata, AccumuloSplitManager splitManager,
            AccumuloRecordSetProvider recordSetProvider,
            AccumuloHandleResolver handleResolver,
            AccumuloPageSinkProvider pageSinkProvider) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager,
                "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager,
                "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider,
                "recordSetProvider is null");
        this.handleResolver = requireNonNull(handleResolver,
                "handleResolver is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider,
                "pageSinkProvider is null");
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        List<PropertyMetadata<?>> properties = new ArrayList<>();

        properties.add(new PropertyMetadata<String>(PROP_COLUMN_MAPPING,
                "Comma-delimited list of column metadata: col_name:col_family:col_qualifier,[...]",
                VarcharType.VARCHAR, String.class, null, false,
                value -> ((String) value).toLowerCase()));

        properties.add(new PropertyMetadata<Boolean>(PROP_INTERNAL,
                "If true, a DROP TABLE statement WILL delete the corresponding Accumulo table.  Default false.",
                BooleanType.BOOLEAN, Boolean.class, false, false));

        properties.add(new PropertyMetadata<Boolean>(PROP_METADATA_ONLY,
                "True to only create metadata about the Accumulo table vs. actually creating the table",
                BooleanType.BOOLEAN, Boolean.class, false, false));

        properties.add(new PropertyMetadata<String>(PROP_SERIALIZER,
                "Serializer for Accumulo data encodings.  Can either be 'default', 'string', 'lexicoder', or a Java class name.  Default is 'default', i.e. the value from AccumuloRowSerializer.getDefault()",
                VarcharType.VARCHAR, String.class,
                AccumuloRowSerializer.getDefault().getClass().getName(), false,
                x -> x.equals("default")
                        ? AccumuloRowSerializer.getDefault().getClass()
                                .getName()
                        : (x.equals("string")
                                ? StringRowSerializer.class.getName()
                                : (x.equals("lexicoder")
                                        ? LexicoderRowSerializer.class.getName()
                                        : (String) x))));

        return properties;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return handleResolver;
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            LOG.error(e, "Error shutting down connector");
        }
    }
}
