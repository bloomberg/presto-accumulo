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

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface AccumuloRowDeserializer {

    public static AccumuloRowDeserializer getDefault() {
        return new StringRowDeserializer();
    }

    public void setMapping(String name, String fam, String qual);

    public void deserialize(Entry<Key, Value> row) throws IOException;

    public boolean isNull(String name);

    public boolean getBoolean(String name);

    public Date getDate(String name);

    public double getDouble(String name);

    public long getLong(String name);

    public Object getObject(String name);

    public Time getTime(String name);

    public Timestamp getTimestamp(String name);

    public byte[] getVarbinary(String name);

    public String getVarchar(String name);
}