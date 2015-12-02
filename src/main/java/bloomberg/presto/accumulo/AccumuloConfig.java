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

import javax.validation.constraints.NotNull;

import io.airlift.configuration.Config;

public class AccumuloConfig {
    private String schema;
    private String table;

    @NotNull
    public String getSchema() {
        return this.schema;
    }

    @Config("schema")
    public AccumuloConfig setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    @NotNull
    public String getTable() {
        return this.table;
    }

    @Config("table")
    public AccumuloConfig setTable(String table) {
        this.table = table;
        return this;
    }
}
