/**
 * Copyright 2016 Bloomberg L.P.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.examples;

import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

@SuppressWarnings("ALL")
public class Constants
{
    public static final String DATA_TABLE = "tpch";
    public static final String INDEX_TABLE = "tpch_index";

    public static final byte[] CF = "cf".getBytes();
    public static final byte[] CUSTKEY = "custkey".getBytes();
    public static final byte[] ORDERSTATUS = "orderstatus".getBytes();
    public static final byte[] TOTALPRICE = "totalprice".getBytes();
    public static final byte[] ORDERDATE = "orderdate".getBytes();
    public static final byte[] ORDERPRIORITY = "orderpriority".getBytes();
    public static final byte[] CLERK = "clerk".getBytes();
    public static final byte[] SHIPPRIORITY = "shippriority".getBytes();
    public static final byte[] COMMENT = "comment".getBytes();
    public static final byte[] EMPTY_BYTES = "".getBytes();

    public static final String CUSTKEY_STR = "custkey";
    public static final String ORDERSTATUS_STR = "orderstatus";
    public static final String TOTALPRICE_STR = "totalprice";
    public static final String ORDERDATE_STR = "orderdate";
    public static final String ORDERPRIORITY_STR = "orderpriority";
    public static final String CLERK_STR = "clerk";
    public static final String SHIPPRIORITY_STR = "shippriority";
    public static final String COMMENT_STR = "comment";

    public static final SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");

    public static final LongLexicoder longLexicoder = new LongLexicoder();
    public static final StringLexicoder strLexicoder = new StringLexicoder();
    public static final DoubleLexicoder dblLexicoder = new DoubleLexicoder();

    public static byte[] encode(Object v)
    {
        if (v instanceof Long) {
            return longLexicoder.encode((Long) v);
        }
        else if (v instanceof String) {
            return strLexicoder.encode((String) v);
        }
        else if (v instanceof Double) {
            return dblLexicoder.encode((Double) v);
        }
        else if (v instanceof Date) {
            return longLexicoder.encode(((Date) v).getTime());
        }
        else {
            throw new UnsupportedOperationException("Unsupported encoding of class " + v.getClass());
        }
    }

    public static <T> T decode(Class<T> type, byte[] bytes)
    {
        if (type.equals(Long.class)) {
            return (T) longLexicoder.decode(bytes);
        }
        else if (type.equals(String.class)) {
            return (T) strLexicoder.decode(bytes);
        }
        else if (type.equals(Double.class)) {
            return (T) dblLexicoder.decode(bytes);
        }
        else if (type.equals(Date.class)) {
            return (T) new Date(longLexicoder.decode(bytes));
        }
        throw new UnsupportedOperationException("Unsupported decoding of class " + type);
    }

    public static <T> T decode(Class<T> type, byte[] bytes, int length)
    {
        if (type.equals(Long.class) || type.equals(Date.class)) {
            return (T) longLexicoder.decode(Arrays.copyOfRange(bytes, 0, length));
        }
        else if (type.equals(String.class)) {
            return (T) strLexicoder.decode(Arrays.copyOfRange(bytes, 0, length));
        }
        else if (type.equals(Double.class)) {
            return (T) dblLexicoder.decode(Arrays.copyOfRange(bytes, 0, length));
        }
        throw new UnsupportedOperationException("Unsupported decoding of class " + type);
    }
}