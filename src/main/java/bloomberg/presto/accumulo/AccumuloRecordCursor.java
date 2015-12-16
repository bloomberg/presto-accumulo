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
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;

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
    private AccumuloRowDeserializer deserializer;

    public AccumuloRecordCursor(List<AccumuloColumnHandle> cHandles,
            Scanner scan) {
        this.columnHandles = cHandles;
        this.scan = scan;

        LOG.debug("Number of column handles is " + cHandles.size());

        // if there are no columns, or the only column is the row ID, then
        // configure a scan iterator/deserializer to only return the row IDs
        if (cHandles.size() == 0 || (cHandles.size() == 1
                && cHandles.get(0).getColumnName().equals(
                        AccumuloColumnMetadataProvider.ROW_ID_COLUMN_NAME))) {
            this.scan.addScanIterator(new IteratorSetting(1, "firstentryiter",
                    FirstEntryInRowIterator.class));
            deserializer = new RowOnlyDeserializer();
            fieldToColumnName = new String[1];
            fieldToColumnName[0] = AccumuloColumnMetadataProvider.ROW_ID_COLUMN_NAME;
        } else {
            Text fam = new Text(), qual = new Text();
            deserializer = AccumuloRowDeserializer.getDefault();
            this.scan.addScanIterator(new IteratorSetting(1,
                    "whole-row-iterator", WholeRowIterator.class));
            fieldToColumnName = new String[cHandles.size()];

            for (int i = 0; i < cHandles.size(); ++i) {
                AccumuloColumnHandle cHandle = cHandles.get(i);
                fieldToColumnName[i] = cHandle.getColumnName();

                if (!cHandle.getColumnName().equals(
                        AccumuloColumnMetadataProvider.ROW_ID_COLUMN_NAME)) {
                    LOG.debug(String.format("Set column mapping %s", cHandle));
                    deserializer.setMapping(cHandle.getColumnName(),
                            cHandle.getColumnFamily(),
                            cHandle.getColumnQualifier());

                    fam.set(cHandle.getColumnFamily());
                    qual.set(cHandle.getColumnQualifier());
                    this.scan.fetchColumn(fam, qual);
                    LOG.debug(String.format(
                            "Column %s maps to Accumulo column %s:%s",
                            cHandle.getColumnName(), fam, qual));
                } else {
                    LOG.debug(String.format("Column %s maps to Accumulo row ID",
                            cHandle.getColumnName()));
                }
            }
        }

        iterator = this.scan.iterator();
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
        return 0;
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
                deserializer.deserialize(iterator.next());
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return deserializer.isNull(fieldToColumnName[field]);
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return deserializer.getBoolean(fieldToColumnName[field]);
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return deserializer.getDouble(fieldToColumnName[field]);
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BIGINT, DATE, TIME);
        return deserializer.getLong(fieldToColumnName[field]);
    }

    @Override
    public Object getObject(int field) {
        return deserializer.getObject(fieldToColumnName[field]);
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, VARCHAR);
        return deserializer.getSlice(fieldToColumnName[field]);
    }

    @Override
    public void close() {
        scan.close();
    }

    private void checkFieldType(int field, Type... expected) {
        Type actual = getType(field);

        boolean equivalent = false;
        for (Type t : expected) {
            equivalent |= actual.equals(t);
        }

        checkArgument(equivalent,
                "Expected field %s to be a type of %s but is %s", field,
                StringUtils.join(expected, ","), actual);
    }

    private static class RowOnlyDeserializer
            implements AccumuloRowDeserializer {
        private Text r = new Text();

        @Override
        public void setMapping(String name, String fam, String qual) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deserialize(Entry<Key, Value> row) throws IOException {
            row.getKey().getRow(r);
        }

        @Override
        public Slice getSlice(String name) {
            return Slices.utf8Slice(r.toString());
        }

        @Override
        public boolean isNull(String name) {
            return false;
        }

        @Override
        public boolean getBoolean(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getObject(String name) {
            throw new UnsupportedOperationException();
        }
    }
}
