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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeColumnFamilyIterator;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Strings;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class AccumuloRecordCursor implements RecordCursor {

    private static final Logger LOG = Logger.get(AccumuloRecordCursor.class);
    private final List<AccumuloColumnHandle> columnHandles;
    private final String[] fieldToColumnName;

    private final String fieldValue = "value";
    private final long totalBytes = fieldValue.getBytes().length;
    private Scanner scan = null;
    private Iterator<Entry<Key, Value>> iterator = null;
    private Map<String, Object> columnValues = new HashMap<>();
    private Text cf = new Text(), cq = new Text(), value = new Text();
    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private long start, end;

    public AccumuloRecordCursor(List<AccumuloColumnHandle> columnHandles,
            Scanner scan) {
        this.columnHandles = columnHandles;
        this.scan = scan;

        IteratorSetting cfg = new IteratorSetting(1, "whole-key-value-iterator",
                WholeColumnFamilyIterator.class);
        this.scan.addScanIterator(cfg);

        fieldToColumnName = new String[columnHandles.size()];
        Text fam = new Text(), qual = new Text();
        for (int i = 0; i < columnHandles.size(); i++) {
            AccumuloColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnName[i] = columnHandle.getColumnName();

            fam.set(columnHandle.getColumnFamily());
            qual.set(columnHandle.getColumnQualifier());
            this.scan.fetchColumn(fam, qual);
            Map<String, String> q2pc = new HashMap<>();
            q2pc.put(qual.toString(), columnHandle.getColumnName());
            f2q2pc.put(fam.toString(), q2pc);
        }

        iterator = this.scan.iterator();
        start = System.nanoTime();
    }

    @Override
    public long getTotalBytes() {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes() {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return end - start;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        try {
            if (iterator.hasNext()) {
                columnValues.clear();

                Entry<Key, Value> row = iterator.next();
                for (Entry<Key, Value> kvp : WholeColumnFamilyIterator
                        .decodeColumnFamily(row.getKey(), row.getValue())
                        .entrySet()) {
                    kvp.getKey().getColumnFamily(cf);
                    kvp.getKey().getColumnQualifier(cq);
                    value.set(kvp.getValue().get());
                    String prestoColumn = f2q2pc.get(cf.toString())
                            .get(cq.toString());
                    columnValues.put(prestoColumn, value.toString());
                    LOG.debug(String.format(
                            "Put cf:cq %s:%s with value %s into presto column %s",
                            cf.toString(), cq.toString(), value.toString(),
                            prestoColumn));
                }
                return true;
            } else {
                end = System.nanoTime();
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getFieldValue(int field) {
        return columnValues.get(fieldToColumnName[field]);
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field).toString());
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field).toString());
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field).toString());
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field).toString());
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnValues.get(fieldToColumnName[field]) == null;
    }

    private void checkFieldType(int field, Type expected) {
        Type actual = getType(field);
        checkArgument(actual.equals(expected),
                "Expected field %s to be type %s but is %s", field, expected,
                actual);
    }

    @Override
    public void close() {
        scan.close();
    }
}
