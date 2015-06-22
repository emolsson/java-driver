/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * DataType simple unit tests.
 */
public class DataTypeTest {

    CodecRegistry codecRegistry = new CodecRegistry();

    ProtocolVersion protocolVersion = TestUtils.getDesiredProtocolVersion();

    static boolean exclude(DataType t) {
        return t.getName() == DataType.Name.COUNTER;
    }

    private static String[] getOutputCQLStringTestData(DataType dt) {
        switch (dt.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return new String[]{ "'foo'", "'fo''o'" };
            case BIGINT:
                return new String[]{ "42", "91294377723", "-133" };
            case TIMESTAMP:
                return new String[]{ "42", "91294377723", "-133", "784041330999" };
            case DATE:
                return new String[]{ "'2014-01-01'", "'1970-01-01'", "'1970-01-01'", "'-5877641-06-23'" };
            case TIME:
                return new String[]{ "'15:00:12.123450000'", "'00:00:00.000000000'", "'15:00:12.123450000'" };
            case BLOB:
                return new String[]{ "0x2450", "0x" };
            case BOOLEAN:
                return new String[]{ "true", "false" };
            case DECIMAL:
                return new String[]{ "1.23E+8" };
            case DOUBLE:
                return new String[]{ "2.39324324", "-12.0" };
            case FLOAT:
                return new String[]{ "2.39", "-12.0" };
            case INET:
                return new String[]{ "'128.2.12.3'" };
            case TINYINT:
                return new String[]{ "-4", "44" };
            case SMALLINT:
                return new String[]{ "-3", "43" };
            case INT:
                return new String[]{ "-2", "42" };
            case TIMEUUID:
                return new String[]{ "fe2b4360-28c6-11e2-81c1-0800200c9a66" };
            case UUID:
                return new String[]{ "fe2b4360-28c6-11e2-81c1-0800200c9a66", "067e6162-3b6f-4ae2-a171-2470b63dff00" };
            case VARINT:
                return new String[]{ "12387290982347987032483422342432" };
            default:
                throw new RuntimeException("Missing handling of " + dt);
        }
    }

    private static String[] getInputCQLStringTestData(DataType dt) {
        switch (dt.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return new String[]{ "'foo'", "'fo''o'" };
            case BIGINT:
                return new String[]{ "42", "91294377723", "-133" };
            case TIMESTAMP:
                // single quotes are optional for long literals, mandatory for date patterns
                return new String[]{ "42", "91294377723", "-133", "'1994-11-05T14:15:30.999+0100'" };
            case DATE:
                // single quotes are optional for long literals, mandatory for date patterns
                return new String[]{ "'2014-01-01'", "'1970-01-01'", "'2147483648'", "0" };
            case TIME:
                // all literals must by enclosed in single quotes
                return new String[]{ "'54012123450000'", "'0'", "'15:00:12.123450000'" };
            case BLOB:
                return new String[]{ "0x2450", "0x" };
            case BOOLEAN:
                return new String[]{ "true", "false" };
            case DECIMAL:
                return new String[]{ "1.23E+8" };
            case DOUBLE:
                return new String[]{ "2.39324324", "-12.0" };
            case FLOAT:
                return new String[]{ "2.39", "-12.0" };
            case INET:
                return new String[]{ "'128.2.12.3'" };
            case TINYINT:
                return new String[]{ "-4", "44" };
            case SMALLINT:
                return new String[]{ "-3", "43" };
            case INT:
                return new String[]{ "-2", "42" };
            case TIMEUUID:
                return new String[]{ "fe2b4360-28c6-11e2-81c1-0800200c9a66" };
            case UUID:
                return new String[]{ "fe2b4360-28c6-11e2-81c1-0800200c9a66", "067e6162-3b6f-4ae2-a171-2470b63dff00" };
            case VARINT:
                return new String[]{ "12387290982347987032483422342432" };
            default:
                throw new RuntimeException("Missing handling of " + dt);
        }
    }

    private static Object[] getTestData(DataType dt) throws Exception {
        switch (dt.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return new Object[]{ "foo", "fo'o" };
            case BIGINT:
                return new Object[]{ 42L, 91294377723L, -133L };
            case TIMESTAMP:
                return new Object[]{
                    new Date(42L),
                    new Date(91294377723L),
                    new Date(-133L),
                    new Date(784041330999L)
                };
            case DATE:
                return new Object[]{
                    LocalDate.fromDaysSinceEpoch(16071) /* 2014-01-01 */,
                    LocalDate.fromDaysSinceEpoch(0) /* 1970-01-01 */,
                    LocalDate.fromDaysSinceEpoch((int)(2147483648L - (1L << 31))),
                    LocalDate.fromDaysSinceEpoch((int)(0 - (1L << 31)))
                };
            case TIME:
                return new Object[]{ 54012123450000L /* 15:00:12.123450000 */, 0L, 54012123450000L };
            case BLOB:
                return new Object[]{ Bytes.fromHexString("0x2450"), ByteBuffer.allocate(0) };
            case BOOLEAN:
                return new Object[]{ true, false };
            case DECIMAL:
                return new Object[]{ new BigDecimal("1.23E+8") };
            case DOUBLE:
                return new Object[]{ 2.39324324, -12. };
            case FLOAT:
                return new Object[]{ 2.39f, -12.f };
            case INET:
                try {
                    return new Object[]{ InetAddress.getByName("128.2.12.3") };
                } catch (java.net.UnknownHostException e) {
                    throw new RuntimeException();
                }
            case TINYINT:
                return new Object[]{ (byte)-4, (byte)44 };
            case SMALLINT:
                return new Object[]{ (short)-3, (short)43 };
            case INT:
                return new Object[]{ -2, 42 };
            case TIMEUUID:
                return new Object[]{ UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66") };
            case UUID:
                return new Object[]{ UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66"), UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00") };
            case VARINT:
                return new Object[]{ new BigInteger("12387290982347987032483422342432") };
            default:
                throw new RuntimeException("Missing handling of " + dt);
        }
    }

    @Test(groups = "unit")
    public void parseNativeTest() throws Exception {
        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (exclude(dt))
                continue;

            String[] s = getInputCQLStringTestData(dt);
            Object[] o = getTestData(dt);
            for (int i = 0; i < s.length; i++) {
                assertEquals(codecRegistry.codecFor(dt).parse(s[i]), o[i], String.format("For input %d of %s", i, dt));
            }
        }
    }

    @Test(groups = "unit")
    public void formatNativeTest() throws Exception {
        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (exclude(dt))
                continue;

            Object[] o = getTestData(dt);
            String[] s = getOutputCQLStringTestData(dt);
            for (int i = 0; i < s.length; i++)
                assertEquals(codecRegistry.codecFor(dt).format(o[i]), s[i], String.format("For input %d of %s", i, dt));
        }
    }

    @Test(groups = "unit")
    public void parseFormatListTest() {
        String toParse = "['Foo','Bar','Foo''bar']";
        List<String> toFormat = Arrays.asList("Foo", "Bar", "Foo'bar");
        DataType dt = DataType.list(DataType.text());
        assertEquals(codecRegistry.codecFor(dt).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(dt).format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatSetTest() {
        String toParse = "{'Foo','Bar','Foo''bar'}";
        Set<String> toFormat = new LinkedHashSet<String>(){{ add("Foo"); add("Bar"); add("Foo'bar"); }};
        DataType dt = DataType.set(DataType.text());
        assertEquals(codecRegistry.codecFor(dt).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(dt).format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatMapTest() {
        String toParse = "{'Foo':3,'Bar':42,'Foo''bar':-24}";
        Map<String, Integer> toFormat = new LinkedHashMap<String, Integer>(){{ put("Foo", 3); put("Bar", 42); put("Foo'bar", -24); }};
        DataType dt = DataType.map(DataType.text(), DataType.cint());
        assertEquals(codecRegistry.codecFor(dt).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(dt).format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatUDTTest() {

        String toParse = "{t:'fo''o',i:3,l:['a','b'],s:{3:{a:0x01}}}";

        final UserType udt1 = new UserType("ks", "t", Arrays.asList(new UserType.Field("a", DataType.blob())), protocolVersion, codecRegistry);
        UserType udt2 = new UserType("ks", "t", Arrays.asList(
            new UserType.Field("t", DataType.text()),
            new UserType.Field("i", DataType.cint()),
            new UserType.Field("l", DataType.list(DataType.text())),
            new UserType.Field("s", DataType.map(DataType.cint(), udt1))
        ), protocolVersion, codecRegistry);

        UDTValue toFormat = udt2.newValue();
        toFormat.setString("t", "fo'o");
        toFormat.setInt("i", 3);
        toFormat.setList("l", Arrays.<String>asList("a", "b"));
        toFormat.setMap("s", new HashMap<Integer, UDTValue>(){{ put(3, udt1.newValue().setBytes("a", ByteBuffer.wrap(new byte[]{1}))); }});

        assertEquals(codecRegistry.codecFor(udt2).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(udt2).format(toFormat), toParse);
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "unit")
    public void parseFormatTupleTest() {

        String toParse = "(1,'foo',1.0)";
        TupleType t = TupleType.of(DataType.cint(), DataType.text(), DataType.cfloat());
        TupleValue toFormat = t.newValue(1, "foo", 1.0f);

        assertEquals(codecRegistry.codecFor(t).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(t).format(toFormat), toParse);
    }

    @Test(groups = "unit")
    public void serializeDeserializeTest() {
        for (ProtocolVersion v : ProtocolVersion.values())
            serializeDeserializeTest(v);
    }

    public void serializeDeserializeTest(ProtocolVersion version) {

        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (exclude(dt))
                continue;

            Object value = TestUtils.getFixedValue(dt);
            TypeCodec<Object> codec = codecRegistry.codecFor(dt);
            assertEquals(codec.deserialize(codec.serialize(value, version), version), value);
        }

        TypeCodec<Long> codec = codecRegistry.codecFor(DataType.bigint());

        try {
            ByteBuffer badValue = ByteBuffer.allocate(4);
            codec.deserialize(badValue, version);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }
    }

    @Test(groups = "unit")
    public void serializeDeserializeCollectionsTest() {
        for (ProtocolVersion v : ProtocolVersion.values())
            serializeDeserializeCollectionsTest(v);
    }

    public void serializeDeserializeCollectionsTest(ProtocolVersion version) {

        List<String> l = Arrays.asList("foo", "bar");

        DataType dt = DataType.list(DataType.text());
        TypeCodec<List<String>> codec = codecRegistry.codecFor(dt);
        assertEquals(codec.deserialize(codec.serialize(l, version), version), l);

        try {
            DataType listOfBigint = DataType.list(DataType.bigint());
            codec = codecRegistry.codecFor(listOfBigint);
            codec.serialize(l, version);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }
    }

    @Test(groups = "unit")
    public void should_not_return_v4_types_in_all_primitive_types_with_v3() {
        Set<DataType> dataTypes = DataType.allPrimitiveTypes(ProtocolVersion.V3);

        // Ensure it does not contain specific newer types tinyint and smallint.
        assertThat(dataTypes).doesNotContainAnyElementsOf(Lists.newArrayList(DataType.tinyint(), DataType.smallint()));

        // Ensure all values are <= V3.
        for(DataType dataType : dataTypes) {
            assertThat(dataType.getName().minProtocolVersion).isLessThanOrEqualTo(ProtocolVersion.V3);
        }
    }

    @Test(groups = "unit")
    public void should_return_same_elements_with_all_primitive_types_using_latest_protocol_version() {
        assertThat(DataType.allPrimitiveTypes(ProtocolVersion.NEWEST_SUPPORTED)).isEqualTo(DataType.allPrimitiveTypes());
    }
}
