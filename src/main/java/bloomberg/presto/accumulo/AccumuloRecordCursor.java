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
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import bloomberg.presto.accumulo.iterators.SingleColumnValueFilter;
import bloomberg.presto.accumulo.iterators.SingleColumnValueFilter.CompareOp;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class AccumuloRecordCursor implements RecordCursor {

    private static final Logger LOG = Logger.get(AccumuloRecordCursor.class);
    private final List<AccumuloColumnHandle> cHandles;
    private final String[] fieldToColumnName;

    // TODO totalBytes stuff
    private final long totalBytes = 0L;
    private final Scanner scan;
    private final Iterator<Entry<Key, Value>> iterator;
    private final AccumuloRowSerializer serializer;
    private final ConnectorSession session;

    public AccumuloRecordCursor(ConnectorSession session,
            AccumuloRowSerializer serializer, Scanner scan,
            List<AccumuloColumnHandle> cHandles,
            List<AccumuloColumnConstraint> constraints) {
        this.session = requireNonNull(session, "session is null");
        this.cHandles = requireNonNull(cHandles, "cHandles is null");
        this.scan = requireNonNull(scan, "scan is null");

        LOG.debug("Number of column handles is " + cHandles.size());

        // if there are no columns, or the only column is the row ID, then
        // configure a scan iterator/serializer to only return the row IDs
        if (cHandles.size() == 0
                || (cHandles.size() == 1 && cHandles.get(0).getName()
                        .equals(AccumuloMetadataManager.ROW_ID_COLUMN_NAME))) {
            this.scan.addScanIterator(new IteratorSetting(Integer.MAX_VALUE,
                    "firstentryiter", FirstEntryInRowIterator.class));

            this.serializer = new RowOnlySerializer();
            fieldToColumnName = new String[1];
            fieldToColumnName[0] = AccumuloMetadataManager.ROW_ID_COLUMN_NAME;
        } else {
            this.serializer = requireNonNull(serializer, "serializer is null");

            Text fam = new Text(), qual = new Text();
            this.scan.addScanIterator(new IteratorSetting(Integer.MAX_VALUE,
                    "whole-row-iterator", WholeRowIterator.class));
            fieldToColumnName = new String[cHandles.size()];

            for (int i = 0; i < cHandles.size(); ++i) {
                AccumuloColumnHandle cHandle = cHandles.get(i);
                fieldToColumnName[i] = cHandle.getName();

                if (!cHandle.getName()
                        .equals(AccumuloMetadataManager.ROW_ID_COLUMN_NAME)) {
                    LOG.debug("Set column mapping %s", cHandle);
                    serializer.setMapping(cHandle.getName(),
                            cHandle.getColumnFamily(),
                            cHandle.getColumnQualifier());

                    fam.set(cHandle.getColumnFamily());
                    qual.set(cHandle.getColumnQualifier());
                    this.scan.fetchColumn(fam, qual);
                    LOG.debug("Column %s maps to Accumulo column %s:%s",
                            cHandle.getName(), fam, qual);
                } else {
                    LOG.debug("Column %s maps to Accumulo row ID",
                            cHandle.getName());
                }
            }
        }

        addColumnIterators(constraints);

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
        checkArgument(field < cHandles.size(), "Invalid field index");
        return cHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        try {
            if (iterator.hasNext()) {
                serializer.deserialize(iterator.next());
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < cHandles.size(), "Invalid field index");
        return serializer.isNull(fieldToColumnName[field]);
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return serializer.getBoolean(fieldToColumnName[field]);
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return serializer.getDouble(fieldToColumnName[field]);
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BIGINT, DATE, TIME, TIMESTAMP);
        switch (getType(field).getDisplayName()) {
        case StandardTypes.BIGINT:
            return serializer.getLong(fieldToColumnName[field]);
        case StandardTypes.DATE:
            return serializer.getDate(fieldToColumnName[field]).getTime();
        case StandardTypes.TIME:
            return serializer.getTime(fieldToColumnName[field]).getTime();
        case StandardTypes.TIMESTAMP:
            return serializer.getTimestamp(fieldToColumnName[field]).getTime();
        default:
            throw new RuntimeException("Unsupported type " + getType(field));
        }
    }

    @Override
    public Object getObject(int field) {
        Type type = getType(field);
        checkArgument(Types.isArrayType(type) || Types.isMapType(type),
                "Expected field %s to be a type of array or map but is %s",
                field, type);

        if (Types.isArrayType(type)) {
            return serializer.getArray(fieldToColumnName[field], type);
        } else {
            return serializer.getMap(fieldToColumnName[field], type);
        }
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, VARBINARY, VARCHAR);
        switch (getType(field).getDisplayName()) {
        case StandardTypes.VARBINARY:
            return Slices.wrappedBuffer(
                    serializer.getVarbinary(fieldToColumnName[field]));
        case StandardTypes.VARCHAR:
            return Slices
                    .utf8Slice(serializer.getVarchar(fieldToColumnName[field]));
        default:
            throw new RuntimeException("Unsupported type " + getType(field));
        }
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

    private void addColumnIterators(
            List<AccumuloColumnConstraint> constraints) {
        int priority = 1;
        for (AccumuloColumnConstraint col : constraints) {
            Domain dom = col.getDomain();

            Logger.get(getClass()).debug("COLUMN %s HAS %d RANGES",
                    col.getName(), dom.getValues().getRanges().getRangeCount());

            for (Range r : dom.getValues().getRanges().getOrderedRanges()) {
                Logger.get(getClass()).debug("RANGE %s", r.toString(session));
                if (r.isAll()) {
                    // [min, max]
                    Logger.get(getClass()).debug("RANGE %s IS ALL",
                            r.toString(session));
                } else if (r.isSingleValue()) {
                    // value = value
                    Logger.get(getClass()).debug("RANGE %s IS SINGLE VALUE",
                            r.toString(session));
                    addSingleValueFilter(priority++, col, CompareOp.EQUAL,
                            r.getType(), r.getSingleValue());
                } else {
                    if (r.getLow().isLowerUnbounded()) {
                        Logger.get(getClass()).debug(
                                "RANGE %s IS LOWER UNBOUNDED",
                                r.toString(session));
                        // (min, x] WHERE x < 10
                        CompareOp op = r.getHigh().getBound() == Bound.EXACTLY
                                ? CompareOp.LESS_OR_EQUAL : CompareOp.LESS;
                        addSingleValueFilter(priority++, col, op, r.getType(),
                                r.getHigh().getValue());
                    } else if (r.getHigh().isUpperUnbounded()) {
                        Logger.get(getClass()).debug(
                                "RANGE %s IS UPPER UNBOUNDED",
                                r.toString(session));
                        // [(x, max] WHERE x > 10
                        CompareOp op = r.getLow().getBound() == Bound.EXACTLY
                                ? CompareOp.GREATER_OR_EQUAL
                                : CompareOp.GREATER;
                        addSingleValueFilter(priority++, col, op, r.getType(),
                                r.getLow().getValue());
                    } else {
                        Logger.get(getClass()).debug("RANGE %s IS BOUNDED",
                                r.toString(session));
                        // WHERE x > 10 AND x < 20
                        CompareOp op = r.getHigh().getBound() == Bound.EXACTLY
                                ? CompareOp.LESS_OR_EQUAL : CompareOp.LESS;
                        addSingleValueFilter(priority++, col, op, r.getType(),
                                r.getHigh().getValue());

                        op = r.getLow().getBound() == Bound.EXACTLY
                                ? CompareOp.GREATER_OR_EQUAL
                                : CompareOp.GREATER;
                        addSingleValueFilter(priority++, col, op, r.getType(),
                                r.getLow().getValue());
                    }
                }
            }

        }
    }

    @SuppressWarnings("unchecked")
    private void addSingleValueFilter(int priority,
            AccumuloColumnConstraint col, CompareOp op, Type type,
            Object value) {

        LOG.debug(String.format("Adding %s filter at value %s", op, value));

        String name = String.format("nullable-single-value:%s:%d",
                col.getName(), priority);
        byte[] valueBytes = LexicoderRowSerializer.getLexicoder(type)
                .encode(value);
        IteratorSetting cfg = new IteratorSetting(priority++, name,
                SingleColumnValueFilter.class,
                SingleColumnValueFilter.getProperties(col.getFamily(),
                        col.getQualifier(), op, valueBytes));

        this.scan.addScanIterator(cfg);
    }

    private static class RowOnlySerializer implements AccumuloRowSerializer {
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
        public boolean isNull(String name) {
            return false;
        }

        @Override
        public Block getArray(String name, Type type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setArray(Text value, Type type, Block block) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBoolean(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBoolean(Text text, Boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getDate(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDate(Text text, Date value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDouble(Text text, Double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLong(Text text, Long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Block getMap(String name, Type type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMap(Text text, Type type, Block block) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Time getTime(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTime(Text text, Time value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Timestamp getTimestamp(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTimestamp(Text text, Timestamp value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getVarbinary(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setVarbinary(Text text, byte[] value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getVarchar(String string) {
            return r.toString();
        }

        @Override
        public void setVarchar(Text text, String value) {
            throw new UnsupportedOperationException();
        }
    }
}
