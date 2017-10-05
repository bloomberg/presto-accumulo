/*
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
package com.facebook.presto.accumulo.tools;

import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.lexicoder.ListLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestSandbox
{
    @Test
    public void test()
            throws Exception
    {
        System.out.println(new String(new byte[] { }, UTF_8));
        String array = "\\x08\\x80\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01d";
        System.out.println(array);
        System.out.println(new ListLexicoder<>(new LongLexicoder()).decode(decodeBytes(array)));
        System.out.println(new LongLexicoder().decode(decodeBytes(array)));
        System.out.println(encodeBytes(new LongLexicoder().encode(100L)));
        System.out.println(encodeBytes(new ListLexicoder<>(new LongLexicoder()).encode(ImmutableList.of(100L))));
    }

    static byte[] decodeBytes(String encoded)
            throws DecoderException
    {
        List<Byte> bytes = new ArrayList<>();

        String tmp = encoded;
        while (tmp.length() > 0) {
            if (tmp.length() >= 2 && tmp.substring(0, 2).equals("\\\\")) {
                bytes.add((byte) '\\');
                tmp = tmp.substring(2);
            }
            else if (tmp.length() >= 2 && tmp.substring(0, 2).equals("\\x")) {
                bytes.add(Hex.decodeHex(tmp.substring(2, 4).toCharArray())[0]);
                tmp = tmp.substring(4);
            }
            else {
                bytes.add((byte) tmp.charAt(0));
                tmp = tmp.substring(1);
            }
        }
        byte[] retval = new byte[bytes.size()];

        int i = 0;
        for (byte b : bytes) {
            retval[i++] = b;
        }
        return retval;
    }

    static String encodeBytes(byte[] ba)
    {
        StringBuilder sb = new StringBuilder();
        for (byte b : ba) {
            int c = 0xff & b;
            if (c == '\\') {
                sb.append("\\\\");
            }
            else if (c >= 32 && c <= 126) {
                sb.append((char) c);
            }
            else {
                sb.append("\\x").append(String.format("%02X", c));
            }
        }
        return sb.toString();
    }
}
