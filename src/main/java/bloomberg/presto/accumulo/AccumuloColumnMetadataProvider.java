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

import java.util.List;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

public abstract class AccumuloColumnMetadataProvider {

    public static final String ROW_ID_COLUMN_NAME = "recordkey";
    public static final Type ROW_ID_COLUMN_TYPE = VarcharType.VARCHAR;

    public static AccumuloColumnMetadataProvider getDefault(
            AccumuloConfig config) {
        return new ZooKeeperColumnMetadataProvider(config);
    }

    public abstract List<AccumuloColumn> getColumnMetadata(String schema,
            String table);

    public abstract AccumuloColumn getAccumuloColumn(String schema,
            String table, String name);

    public AccumuloColumn getRowIdColumn() {
        return new AccumuloColumn(ROW_ID_COLUMN_NAME, null, null,
                ROW_ID_COLUMN_TYPE);
    }
}
