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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.concat;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.escape;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.split;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.unescape;

/**
 * Combines the byte arrays that were encoded by a ListLexicoder into a single list containing those elements.
 * Order is not guaranteed.
 */
public class ListCombiner
        extends Combiner
{
    @Override
    public Value reduce(Key key, Iterator<Value> iter)
    {
        // Iterate through all values, decoding the byte array of elements into
        List<byte[]> v = new ArrayList<>();
        while (iter.hasNext()) {
            v.addAll(decode(iter.next().get()));
        }

        // And now we'll encode the list of values back together
        return new Value(encode(v));
    }

    private List<byte[]> decode(byte[] b)
    {
        byte[][] escapedElements = split(b, 0, b.length);
        List<byte[]> ret = new ArrayList<>(escapedElements.length);

        for (byte[] escapedElement : escapedElements) {
            ret.add(unescape(escapedElement));
        }

        return ret;
    }

    private byte[] encode(List<byte[]> v)
    {
        byte[][] encElements = new byte[v.size()][];

        int index = 0;
        for (byte[] element : v) {
            encElements[index++] = escape(element);
        }

        return concat(encElements);
    }
}
