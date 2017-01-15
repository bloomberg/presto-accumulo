/**
 * Copyright 2016 Bloomberg L.P.
 *
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
package com.facebook.presto.accumulo.iterators;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A filter that ages off key/value pairs based on the Key's column and timestamp. It removes an entry if its timestamp is less than a set value.
 */
public class ColumnTimestampFilter
        extends Filter
{
    public static class TimestampSet
            extends ColumnToClassMapping<Long>
    {
        public TimestampSet(Map<String, String> objectStrings)
        {
            super();

            for (Entry<String, String> entry : objectStrings.entrySet()) {
                if (entry.getKey().isEmpty() || entry.getValue().isEmpty()) {
                    continue;
                }

                String column = entry.getKey();
                long timestamp = Long.parseLong(entry.getValue());

                Pair<Text, Text> colPair = ColumnSet.decodeColumns(column);

                if (colPair.getSecond() == null) {
                    addObject(colPair.getFirst(), timestamp);
                }
                else {
                    addObject(colPair.getFirst(), colPair.getSecond(), timestamp);
                }
            }
        }
    }

    TimestampSet timestampSet;

    @Override
    public boolean accept(Key k, Value v)
    {
        Long timestamp = timestampSet.getObject(k);
        if (timestamp == null) {
            return true;
        }

        // If this key's timestamp is greater than the set timestamp, we will keep it
        return k.getTimestamp() > timestamp;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        super.init(source, options, env);
        this.timestampSet = new TimestampSet(options);
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env)
    {
        ColumnTimestampFilter copy = (ColumnTimestampFilter) super.deepCopy(env);
        copy.timestampSet = timestampSet;
        return copy;
    }

    @Override
    public IteratorOptions describeOptions()
    {
        IteratorOptions io = super.describeOptions();
        io.setName("coltimestampfilter");
        io.setDescription("ColumnTimestampFilter prunes columns at different rates given a timestamp value to for each column");
        io.addUnnamedOption("<col fam>[:<col qual>] <Long> (escape non-alphanum chars using %<hex>)");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options)
    {
        if (!super.validateOptions(options)) {
            return false;
        }

        try {
            this.timestampSet = new TimestampSet(options);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Bad timestamp options: " + e.getMessage(), e);
        }

        return true;
    }

    /**
     * A convenience method for adding or changing a timestamp setting for a column
     *
     * @param is IteratorSetting object to configure
     * @param column column to encode as a parameter name
     * @param timestamp Timestamp age off threshold in milliseconds
     */
    public static void addTimestamp(IteratorSetting is, IteratorSetting.Column column, long timestamp)
    {
        is.addOption(ColumnSet.encodeColumns(column.getFirst(), column.getSecond()), Long.toString(timestamp));
    }

    /**
     * A convenience method for removing a timestamp setting for a column
     *
     * @param is IteratorSetting object to configure
     * @param column column to encode as a parameter name
     */
    public static void removeTimestamp(IteratorSetting is, IteratorSetting.Column column)
    {
        is.removeOption(ColumnSet.encodeColumns(column.getFirst(), column.getSecond()));
    }
}
