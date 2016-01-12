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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import bloomberg.presto.accumulo.iterators.AndFilter;
import bloomberg.presto.accumulo.iterators.NullRowFilter;
import bloomberg.presto.accumulo.iterators.OrFilter;
import bloomberg.presto.accumulo.iterators.SingleColumnValueFilter;
import bloomberg.presto.accumulo.iterators.SingleColumnValueFilter.CompareOp;
import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class AccumuloRecordCursor implements RecordCursor {
    private final List<AccumuloColumnHandle> cHandles;
    private final String[] fieldToColumnName;

    // TODO totalBytes stuff
    private final long totalBytes = 0L;
    private final Scanner scan;
    private final Iterator<Entry<Key, Value>> iterator;
    private final AccumuloRowSerializer serializer;
    private Entry<Key, Value> prevKV;
    private Text prevRowID = new Text();
    private Text rowID = new Text();

    public AccumuloRecordCursor(AccumuloRowSerializer serializer, Scanner scan,
            String rowIdName, List<AccumuloColumnHandle> cHandles,
            List<AccumuloColumnConstraint> constraints) {
        this.cHandles = requireNonNull(cHandles, "cHandles is null");
        this.scan = requireNonNull(scan, "scan is null");

        this.serializer = requireNonNull(serializer, "serializer is null");
        this.serializer.setRowIdName(rowIdName);

        // if there are no columns, or the only column is the row ID, then
        // configure a scan iterator to only return the row IDs
        if (cHandles.size() == 0 || (cHandles.size() == 1
                && cHandles.get(0).getName().equals(rowIdName))) {
            this.scan.addScanIterator(new IteratorSetting(1, "firstentryiter",
                    FirstEntryInRowIterator.class));

            fieldToColumnName = new String[1];
            fieldToColumnName[0] = rowIdName;
            this.serializer.setRowOnly(true);
        } else {
            Text fam = new Text(), qual = new Text();
            fieldToColumnName = new String[cHandles.size()];

            for (int i = 0; i < cHandles.size(); ++i) {
                AccumuloColumnHandle cHandle = cHandles.get(i);
                fieldToColumnName[i] = cHandle.getName();

                if (!cHandle.getName().equals(rowIdName)) {
                    this.serializer.setMapping(cHandle.getName(),
                            cHandle.getColumnFamily(),
                            cHandle.getColumnQualifier());

                    fam.set(cHandle.getColumnFamily());
                    qual.set(cHandle.getColumnQualifier());
                    this.scan.fetchColumn(fam, qual);
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
            // if the iterator doesn't have any more values
            if (!iterator.hasNext()) {
                // deserialize final KV pair
                // this accounts for the edge case when the last read KV pair
                // was a new row and we broke out of the below loop
                if (prevKV != null) {
                    serializer.reset();
                    serializer.deserialize(prevKV);
                    prevKV = null;
                    return true;
                } else {
                    // else we are super done
                    return false;
                }
            }

            // deserialize the previous KV pair, resetting the serializer first
            // this occurs when we need to switch to a new row
            if (prevRowID.getLength() != 0) {
                serializer.reset();
                serializer.deserialize(prevKV);
            }

            boolean braek = false;
            while (iterator.hasNext() && !braek) {
                Entry<Key, Value> kv = iterator.next();
                kv.getKey().getRow(rowID);
                // if the row IDs are equivalent (or there is no previous row
                // ID), deserialize the KV pairs
                if (rowID.equals(prevRowID) || prevRowID.getLength() == 0) {
                    serializer.deserialize(kv);
                } else {
                    // if they are different, we need to break out of the loop
                    braek = true;
                }
                // set our 'previous' member variables
                prevKV = kv;
                prevRowID.set(rowID);
            }

            // if we didn't break out of the loop, we ran out of KV pairs
            // which means we have deserialized all of them
            if (!braek) {
                prevKV = null;
            }

            // return true to process the row
            return true;
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
        AtomicInteger priority = new AtomicInteger(1);
        List<IteratorSetting> allSettings = new ArrayList<>();
        for (AccumuloColumnConstraint col : constraints) {
            Domain dom = col.getDomain();
            List<IteratorSetting> colSettings = new ArrayList<>();

            if (dom.isNullAllowed()) {
                colSettings.add(getNullFilterSetting(col, priority));
            }

            if (Types.isMapType(dom.getType())) {
                // map types are discrete values
                for (Object o : dom.getValues().getDiscreteValues()
                        .getValues()) {
                    IteratorSetting cfg = getIteratorSetting(priority, col,
                            CompareOp.EQUAL, dom.getType(), (Block) o);
                    if (cfg != null) {
                        colSettings.add(cfg);
                    }
                }
            } else {
                // for non-map types (or so it seems), all ranges are ordered
                for (Range r : dom.getValues().getRanges().getOrderedRanges()) {
                    IteratorSetting cfg = getFilterSettingFromRange(col, r,
                            priority);
                    if (cfg != null) {
                        colSettings.add(cfg);
                    }
                }
            }

            if (colSettings.size() == 1) {
                allSettings.add(colSettings.get(0));
            } else if (colSettings.size() > 0) {
                IteratorSetting ore = OrFilter.orFilters(
                        priority.getAndIncrement(),
                        colSettings.toArray(new IteratorSetting[0]));
                allSettings.add(ore);
            } // else no-op
        }

        if (allSettings.size() == 1) {
            this.scan.addScanIterator(allSettings.get(0));
        } else if (allSettings.size() > 0) {
            this.scan.addScanIterator(AndFilter.andFilters(1, allSettings));
        }
    }

    private IteratorSetting getNullFilterSetting(AccumuloColumnConstraint col,
            AtomicInteger priority) {

        String name = String.format("%s:%d", col.getName(), priority.get());
        return new IteratorSetting(priority.getAndIncrement(), name,
                NullRowFilter.class, NullRowFilter
                        .getProperties(col.getFamily(), col.getQualifier()));
    }

    private IteratorSetting getFilterSettingFromRange(
            AccumuloColumnConstraint col, Range r, AtomicInteger priority) {

        if (r.isAll()) {
            // [min, max]
            return null;
        } else if (r.isSingleValue()) {
            // value = value
            return getIteratorSetting(priority, col, CompareOp.EQUAL,
                    r.getType(), r.getSingleValue());
        } else {
            if (r.getLow().isLowerUnbounded()) {
                // (min, x] WHERE x < 10
                CompareOp op = r.getHigh().getBound() == Bound.EXACTLY
                        ? CompareOp.LESS_OR_EQUAL : CompareOp.LESS;
                return getIteratorSetting(priority, col, op, r.getType(),
                        r.getHigh().getValue());
            } else if (r.getHigh().isUpperUnbounded()) {
                // [(x, max] WHERE x > 10
                CompareOp op = r.getLow().getBound() == Bound.EXACTLY
                        ? CompareOp.GREATER_OR_EQUAL : CompareOp.GREATER;
                return getIteratorSetting(priority, col, op, r.getType(),
                        r.getLow().getValue());
            } else {
                // WHERE x > 10 AND x < 20
                CompareOp op = r.getHigh().getBound() == Bound.EXACTLY
                        ? CompareOp.LESS_OR_EQUAL : CompareOp.LESS;

                IteratorSetting high = getIteratorSetting(priority, col, op,
                        r.getType(), r.getHigh().getValue());

                op = r.getLow().getBound() == Bound.EXACTLY
                        ? CompareOp.GREATER_OR_EQUAL : CompareOp.GREATER;

                IteratorSetting low = getIteratorSetting(priority, col, op,
                        r.getType(), r.getLow().getValue());

                return AndFilter.andFilters(priority.getAndIncrement(), high,
                        low);
            }
        }
    }

    private IteratorSetting getIteratorSetting(AtomicInteger priority,
            AccumuloColumnConstraint col, CompareOp op, Type type,
            Object value) {
        String name = String.format("%s:%d", col.getName(), priority.get());
        byte[] valueBytes = LexicoderRowSerializer.encode(type, value);

        return new IteratorSetting(priority.getAndIncrement(), name,
                SingleColumnValueFilter.class,
                SingleColumnValueFilter.getProperties(col.getFamily(),
                        col.getQualifier(), op, valueBytes));
    }
}
