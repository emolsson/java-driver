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
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.CodecUtils.mapOf;
import static com.datastax.driver.core.CodecUtils.setOf;
import static com.datastax.driver.core.DataType.*;

/**
 * A Codec that can serialize and deserialize to and from a given
 * {@link #getCqlType() CQL type} and a given {@link #getJavaType() Java Type}.
 * <p>
 * <h3>Serializing and deserializing</h3>
 * <p>
 * Two methods handle the serialization and deserialization of Java types into
 * CQL types according to the native protocol specifications:
 * <ol>
 *     <li>{@link #serialize(Object,ProtocolVersion)}: used to serialize from the codec's Java type to a
 *     {@link ByteBuffer} instance corresponding to the codec's CQL type;</li>
 *     <li>{@link #deserialize(ByteBuffer,ProtocolVersion)}}: used to deserialize a {@link ByteBuffer} instance
 *     corresponding to the codec's CQL type to the codec's Java type.</li>
 * </ol>
 * <h3>Formatting and parsing</h3>
 * <p>
 * Two methods handle the formatting and parsing of Java types into
 * CQL strings:
 * <ol>
 *     <li>{@link #format(Object)}: formats the Java type handled by the codec as a CQL string;</li>
 *     <li>{@link #parse(String)}; parses a CQL string into the Java type handled by the codec.</li>
 * </ol>
 * <h3>Inspection</h3>
 * <p>
 * Codecs also have the following inspection methods:
 *
 * <ol>
 *     <li>{@link #accepts(DataType)}}}: returns true if the codec can deserialize the given CQL type;</li>
 *     <li>{@link #accepts(TypeToken)}}: returns true if the codec can serialize the given Java type;</li>
 *     <li>{@link #accepts(Object)}}; returns true if the codec can serialize the given object.</li>
 * </ol>
 *
 * @param <T> The codec's Java type
 */
public abstract class TypeCodec<T> {

    private static final Map<TypeToken<?>, TypeToken<?>> primitiveToWrapperMap = ImmutableMap.<TypeToken<?>, TypeToken<?>>builder()
        .put(TypeToken.of(Boolean.TYPE)  , TypeToken.of(Boolean.class))
        .put(TypeToken.of(Byte.TYPE)     , TypeToken.of(Byte.class))
        .put(TypeToken.of(Character.TYPE), TypeToken.of(Character.class))
        .put(TypeToken.of(Short.TYPE)    , TypeToken.of(Short.class))
        .put(TypeToken.of(Integer.TYPE)  , TypeToken.of(Integer.class))
        .put(TypeToken.of(Long.TYPE)     , TypeToken.of(Long.class))
        .put(TypeToken.of(Double.TYPE)   , TypeToken.of(Double.class))
        .put(TypeToken.of(Float.TYPE)    , TypeToken.of(Float.class))
        .build();
    
    protected final TypeToken<T> javaType;

    protected final DataType cqlType;

    /**
     * This constructor can only be used for non parameterized types.
     * For parameterized ones, please use {@link #TypeCodec(DataType, TypeToken)} instead.
     *
     * @param javaClass The Java class this codec serializes from and deserializes to.
     */
    protected TypeCodec(DataType cqlType, Class<T> javaClass) {
        this(cqlType, TypeToken.of(javaClass));
    }

    protected TypeCodec(DataType cqlType, TypeToken<T> javaType) {
        this.cqlType = cqlType;
        this.javaType = javaType;
    }

    /**
     * Return the Java type that this codec deserializes to and serializes from.
     * @return The Java type this codec deserializes to and serializes from.
     */
    public TypeToken<T> getJavaType() {
        return javaType;
    }

    /**
     * Return the CQL type that this codec deserializes from and serializes to.
     * @return The Java type this codec deserializes from and serializes to.
     */
    public DataType getCqlType() {
        return cqlType;
    }

    /**
     * Serialize the given value according to the CQL type
     * handled by this codec.
     * @param value An instance of T
     * @param protocolVersion the protocol version to use when serializing
     * {@code bytes}. In most cases, the proper value to provide for this argument
     * is the value returned by {@link ProtocolOptions#getProtocolVersion} (which
     * is the protocol version in use by the driver).
     * @return A {@link ByteBuffer} instance containing the serialized form of T
     * @throws InvalidTypeException if the given value does not have the expected type
     * @throws NullPointerException if {@code value} is {@code null}, of for collections,
     * if a collection element is {@code null}
     */
    public abstract ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException;

    /**
     * Deserialize the given {@link ByteBuffer} instance according to the CQL type
     * handled by this codec.
     * Implementation note: codecs for CQL collection types should avoid returning {@code null};
     * they should rather return empty collections instead.
     * @param bytes A {@link ByteBuffer} instance containing the serialized form of T
     * @param protocolVersion the protocol version to use when serializing
     * {@code bytes}. In most cases, the proper value to provide for this argument
     * is the value returned by {@link ProtocolOptions#getProtocolVersion} (which
     * is the protocol version in use by the driver).
     * @return An instance of T
     * @throws InvalidTypeException if the given {@link ByteBuffer} instance cannot be deserialized
     * @throws NullPointerException if {@code bytes} is {@code null}
     */
    public abstract T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException;

    /**
     * Parse the given CQL string into an instance of the Java type
     * handled by this codec.
     * @param value CQL string
     * @return An instance of T
     * @throws InvalidTypeException if the given value cannot be parsed into the expected type
     * @throws NullPointerException if {@code value} is {@code null}
     */
    public abstract T parse(String value) throws InvalidTypeException;

    /**
     * Format the given value as a valid CQL string according
     * to the CQL type handled by this codec.
     * @param value An instance of T
     * @return CQL string
     * @throws InvalidTypeException if the given value does not have the expected type
     * @throws NullPointerException if {@code value} is {@code null}
     */
    public abstract String format(T value) throws InvalidTypeException;

    /**
     * Return {@code true} if this codec is capable of serializing
     * the given {@code javaType}.
     *
     * @param javaType The Java type this codec should serialize from and deserialize to; cannot be {@code null}.
     * @return {@code true} if the codec is capable of serializing
     * the given {@code javaType}, and {@code false} otherwise.
     * @throws NullPointerException if {@code javaType} is {@code null}.
     */
    public boolean accepts(TypeToken javaType) {
        checkNotNull(javaType);
        if(javaType.isPrimitive()) {
            javaType = primitiveToWrapperMap.get(javaType);
        }
        return this.javaType.isAssignableFrom(javaType);
    }

    /**
     * Return {@code true} if this codec is capable of deserializing
     * the given {@code cqlType}.
     *
     * @param cqlType The CQL type this codec should deserialize from and serialize to; cannot be {@code null}.
     * @return {@code true} if the codec is capable of deserializing
     * the given {@code cqlType}, and {@code false} otherwise.
     * @throws NullPointerException if {@code javaType} is {@code null}.
     */
    public boolean accepts(DataType cqlType) {
        checkNotNull(cqlType);
        // text is merely an alias for varchar
        if (cqlType == text())
            cqlType = varchar();
        return this.cqlType.equals(cqlType);
    }

    /**
     * Return {@code true} if this codec is capable of serializing
     * the given object. Note that the object's Java type is inferred
     * from the object' runtime (raw) type, contrary
     * to {@link #accepts(TypeToken)} which is capable of
     * handling generic types.
     * <p>
     * This method is intended mostly to be used by the QueryBuilder
     * when no type information is available when the codec is used.
     * <p>
     * Implementation notes:
     *
     * <ol>
     * <li>The implementation provided here can only handle non-parameterized types;
     * codecs handling parameterized types, such as collection types, must override
     * this method and perform some sort of "manual"
     * inspection of the actual type parameters, because
     * this information is lost at runtime by type erasure and cannot
     * be retrieved from a simple Object parameter.</li>
     * </ol>
     *
     * @param value The Java type this codec should serialize from and deserialize to; cannot be {@code null}.
     * @return {@code true} if the codec is capable of serializing
     * the given {@code javaType}, and {@code false} otherwise.
     * @throws NullPointerException if {@code javaType} is {@code null}.
     */
    public boolean accepts(Object value) {
        checkNotNull(value);
        return accepts(TypeToken.of(value.getClass()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof TypeCodec))
            return false;
        TypeCodec<?> typeCodec = (TypeCodec<?>)o;
        return Objects.equal(javaType, typeCodec.javaType) && Objects.equal(cqlType, typeCodec.cqlType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(javaType, cqlType);
    }

    @Override
    public String toString() {
        return String.format("%s [%s <-> %s]", this.getClass().getSimpleName(), cqlType, javaType);
    }

    public interface PrimitiveBooleanCodec {

        ByteBuffer serializeNoBoxing(boolean v, ProtocolVersion protocolVersion);
        boolean deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

    }

    public interface PrimitiveByteCodec {

        ByteBuffer serializeNoBoxing(byte v, ProtocolVersion protocolVersion);
        byte deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

    }

    public interface PrimitiveShortCodec {

        ByteBuffer serializeNoBoxing(short v, ProtocolVersion protocolVersion);
        short deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

    }

    public interface PrimitiveIntCodec {

        ByteBuffer serializeNoBoxing(int v, ProtocolVersion protocolVersion);
        int deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

    }

    public interface PrimitiveLongCodec {

        ByteBuffer serializeNoBoxing(long v, ProtocolVersion protocolVersion);
        long deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

    }

    public interface PrimitiveFloatCodec {

        ByteBuffer serializeNoBoxing(float v, ProtocolVersion protocolVersion);
        float deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

    }

    public interface PrimitiveDoubleCodec {

        ByteBuffer serializeNoBoxing(double v, ProtocolVersion protocolVersion);
        double deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

    }

    public static abstract class StringCodec extends TypeCodec<String> {

        private final Charset charset;

        public StringCodec(DataType cqlType, Charset charset) {
            super(cqlType, String.class);
            this.charset = charset;
        }

        @Override
        public String parse(String value) {
            if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
                throw new InvalidTypeException("text or varchar values must be enclosed by single quotes");

            return value.substring(1, value.length() - 1).replace("''", "'");
        }

        @Override
        public String format(String value) {
            return '\'' + replace(value, '\'', "''") + '\'';
        }

        // Simple method to replace a single character. String.replace is a bit too
        // inefficient (see JAVA-67)
        static String replace(String text, char search, String replacement) {
            if (text == null || text.isEmpty())
                return text;

            int nbMatch = 0;
            int start = -1;
            do {
                start = text.indexOf(search, start + 1);
                if (start != -1)
                    ++nbMatch;
            } while (start != -1);

            if (nbMatch == 0)
                return text;

            int newLength = text.length() + nbMatch * (replacement.length() - 1);
            char[] result = new char[newLength];
            int newIdx = 0;
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (c == search) {
                    for (int r = 0; r < replacement.length(); r++)
                        result[newIdx++] = replacement.charAt(r);
                } else {
                    result[newIdx++] = c;
                }
            }
            return new String(result);
        }

        @Override
        public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) {
            return ByteBuffer.wrap(value.getBytes(charset));
        }

        @Override
        public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return new String(Bytes.getArray(bytes), charset);
        }
    }

    public static class VarcharCodec extends StringCodec {

        public static final VarcharCodec instance = new VarcharCodec();

        private VarcharCodec() {
            super(varchar(), Charset.forName("UTF-8"));
        }

    }

    public static class AsciiCodec extends StringCodec {

        public static final AsciiCodec instance = new AsciiCodec();

        private static final Pattern ASCII_PATTERN = Pattern.compile("^\\p{ASCII}*$");

        private AsciiCodec() {
            super(ascii(), Charset.forName("US-ASCII"));
        }

        @Override
        public boolean accepts(Object value) {
            if (value instanceof String) {
                return ASCII_PATTERN.matcher((String)value).matches();
            }
            return false;
        }

    }

    public abstract static class LongCodec extends TypeCodec<Long> implements PrimitiveLongCodec {

        public LongCodec(DataType cqlType) {
            super(cqlType, Long.class);
        }

        @Override
        public Long parse(String value) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits long value from \"%s\"", value));
            }
        }

        @Override
        public String format(Long value) {
            return Long.toString(value);
        }

        @Override
        public ByteBuffer serialize(Long value, ProtocolVersion protocolVersion) {
            return serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public ByteBuffer serializeNoBoxing(long value, ProtocolVersion protocolVersion) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong(0, value);
            return bb;
        }

        @Override
        public Long deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return deserializeNoBoxing(bytes, protocolVersion);
        }

        @Override
        public long deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes.remaining() != 8)
                throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());

            return bytes.getLong(bytes.position());
        }
    }

    public static class BigintCodec extends LongCodec {

        public static final BigintCodec instance = new BigintCodec();

        private BigintCodec() {
            super(bigint());
        }

    }

    public static class CounterCodec extends LongCodec {

        public static final CounterCodec instance = new CounterCodec();

        private CounterCodec() {
            super(counter());
        }

    }

    public static class BlobCodec extends TypeCodec<ByteBuffer> {

        public static final BlobCodec instance = new BlobCodec();

        private BlobCodec() {
            super(blob(), ByteBuffer.class);
        }

        @Override
        public ByteBuffer parse(String value) {
            return Bytes.fromHexString(value);
        }

        @Override
        public String format(ByteBuffer value) {
            return Bytes.toHexString(value);
        }

        @Override
        public ByteBuffer serialize(ByteBuffer value, ProtocolVersion protocolVersion) {
            return value.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return bytes.duplicate();
        }
    }

    public static class CustomCodec extends TypeCodec<ByteBuffer> {

        public CustomCodec(DataType custom) {
            super(custom, ByteBuffer.class);
            assert custom.getName() == Name.CUSTOM;
        }

        @Override
        public ByteBuffer parse(String value) {
            return Bytes.fromHexString(value);
        }

        @Override
        public String format(ByteBuffer value) {
            return Bytes.toHexString(value);
        }

        @Override
        public ByteBuffer serialize(ByteBuffer value, ProtocolVersion protocolVersion) {
            return value.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return bytes.duplicate();
        }
    }

    public static class BooleanCodec extends TypeCodec<Boolean> implements PrimitiveBooleanCodec {

        private static final ByteBuffer TRUE = ByteBuffer.wrap(new byte[]{ 1 });
        private static final ByteBuffer FALSE = ByteBuffer.wrap(new byte[]{ 0 });

        public static final BooleanCodec instance = new BooleanCodec();

        private BooleanCodec() {
            super(cboolean(), Boolean.class);
        }

        @Override
        public Boolean parse(String value) {
            if (value.equalsIgnoreCase(Boolean.FALSE.toString()))
                return false;
            if (value.equalsIgnoreCase(Boolean.TRUE.toString()))
                return true;

            throw new InvalidTypeException(String.format("Cannot parse boolean value from \"%s\"", value));
        }

        @Override
        public String format(Boolean value) {
            return value ? "true" : "false";
        }

        @Override
        public ByteBuffer serialize(Boolean value, ProtocolVersion protocolVersion) {
            return serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public ByteBuffer serializeNoBoxing(boolean value, ProtocolVersion protocolVersion) {
            return value ? TRUE.duplicate() : FALSE.duplicate();
        }

        @Override
        public Boolean deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return deserializeNoBoxing(bytes, protocolVersion);
        }

        @Override
        public boolean deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes.remaining() != 1)
                throw new InvalidTypeException("Invalid boolean value, expecting 1 byte but got " + bytes.remaining());

            return bytes.get(bytes.position()) != 0;
        }
    }

    public static class DecimalCodec extends TypeCodec<BigDecimal> {

        public static final DecimalCodec instance = new DecimalCodec();

        private DecimalCodec() {
            super(decimal(), BigDecimal.class);
        }

        @Override
        public BigDecimal parse(String value) {
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse decimal value from \"%s\"", value));
            }
        }

        @Override
        public String format(BigDecimal value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(BigDecimal value, ProtocolVersion protocolVersion) {
            BigInteger bi = value.unscaledValue();
            int scale = value.scale();
            byte[] bibytes = bi.toByteArray();

            ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
            bytes.putInt(scale);
            bytes.put(bibytes);
            bytes.rewind();
            return bytes;
        }

        @Override
        public BigDecimal deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes.remaining() < 4)
                throw new InvalidTypeException("Invalid decimal value, expecting at least 4 bytes but got " + bytes.remaining());

            bytes = bytes.duplicate();
            int scale = bytes.getInt();
            byte[] bibytes = new byte[bytes.remaining()];
            bytes.get(bibytes);

            BigInteger bi = new BigInteger(bibytes);
            return new BigDecimal(bi, scale);
        }
    }

    public static class DoubleCodec extends TypeCodec<Double> implements PrimitiveDoubleCodec {

        public static final DoubleCodec instance = new DoubleCodec();

        private DoubleCodec() {
            super(cdouble(), Double.class);
        }

        @Override
        public Double parse(String value) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits double value from \"%s\"", value));
            }
        }

        @Override
        public String format(Double value) {
            return Double.toString(value);
        }

        @Override
        public ByteBuffer serialize(Double value, ProtocolVersion protocolVersion) {
            return serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public ByteBuffer serializeNoBoxing(double value, ProtocolVersion protocolVersion) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putDouble(0, value);
            return bb;
        }

        @Override
        public Double deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return deserializeNoBoxing(bytes, protocolVersion);
        }

        @Override
        public double deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes.remaining() != 8)
                throw new InvalidTypeException("Invalid 64-bits double value, expecting 8 bytes but got " + bytes.remaining());

            return bytes.getDouble(bytes.position());
        }
    }

    public static class FloatCodec extends TypeCodec<Float> implements PrimitiveFloatCodec {

        public static final FloatCodec instance = new FloatCodec();

        private FloatCodec() {
            super(cfloat(), Float.class);
        }

        @Override
        public Float parse(String value) {
            try {
                return Float.parseFloat(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 32-bits float value from \"%s\"", value));
            }
        }

        @Override
        public String format(Float value) {
            return Float.toString(value);
        }

        @Override
        public ByteBuffer serialize(Float value, ProtocolVersion protocolVersion) {
            return serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public ByteBuffer serializeNoBoxing(float value, ProtocolVersion protocolVersion) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putFloat(0, value);
            return bb;
        }

        @Override
        public Float deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return deserializeNoBoxing(bytes, protocolVersion);
        }

        @Override
        public float deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes.remaining() != 4)
                throw new InvalidTypeException("Invalid 32-bits float value, expecting 4 bytes but got " + bytes.remaining());

            return bytes.getFloat(bytes.position());
        }
    }

    public static class InetCodec extends TypeCodec<InetAddress> {

        public static final InetCodec instance = new InetCodec();

        private InetCodec() {
            super(inet(), InetAddress.class);
        }

        @Override
        public InetAddress parse(String value) {
            value = value.trim();
            if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
                throw new InvalidTypeException(String.format("inet values must be enclosed in single quotes (\"%s\")", value));
            try {
                return InetAddress.getByName(value.substring(1, value.length() - 1));
            } catch (Exception e) {
                throw new InvalidTypeException(String.format("Cannot parse inet value from \"%s\"", value));
            }
        }

        @Override
        public String format(InetAddress value) {
            return "'" + value.getHostAddress() + "'";
        }

        @Override
        public ByteBuffer serialize(InetAddress value, ProtocolVersion protocolVersion) {
            return ByteBuffer.wrap(value.getAddress());
        }

        @Override
        public InetAddress deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            try {
                return InetAddress.getByAddress(Bytes.getArray(bytes));
            } catch (UnknownHostException e) {
                throw new InvalidTypeException("Invalid bytes for inet value, got " + bytes.remaining() + " bytes");
            }
        }
    }

    public static class TinyIntCodec extends TypeCodec<Byte> implements PrimitiveByteCodec {

        public static final TinyIntCodec instance = new TinyIntCodec();

        private TinyIntCodec() {
            super(tinyint(), Byte.class);
        }

        @Override
        public Byte parse(String value) {
            try {
                return Byte.parseByte(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 8-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Byte value) {
            return Byte.toString(value);
        }

        @Override
        public ByteBuffer serialize(Byte value, ProtocolVersion protocolVersion) {
            return serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public ByteBuffer serializeNoBoxing(byte value, ProtocolVersion protocolVersion) {
            ByteBuffer bb = ByteBuffer.allocate(1);
            bb.put(0, value);
            return bb;
        }

        @Override
        public Byte deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return deserializeNoBoxing(bytes, protocolVersion);
        }

        @Override
        public byte deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes.remaining() != 1)
                throw new InvalidTypeException("Invalid 8-bits integer value, expecting 1 byte but got " + bytes.remaining());

            return bytes.get(bytes.position());
        }
    }

    public static class SmallIntCodec extends TypeCodec<Short> implements PrimitiveShortCodec {

        public static final SmallIntCodec instance = new SmallIntCodec();

        private SmallIntCodec() {
            super(smallint(), Short.class);
        }

        @Override
        public Short parse(String value) {
            try {
                return Short.parseShort(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 16-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Short value) {
            return Short.toString(value);
        }

        @Override
        public ByteBuffer serialize(Short value, ProtocolVersion protocolVersion) {
            return serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public ByteBuffer serializeNoBoxing(short value, ProtocolVersion protocolVersion) {
            ByteBuffer bb = ByteBuffer.allocate(2);
            bb.putShort(0, value);
            return bb;
        }

        @Override
        public Short deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return deserializeNoBoxing(bytes, protocolVersion);
        }

        @Override
        public short deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes.remaining() != 2)
                throw new InvalidTypeException("Invalid 16-bits integer value, expecting 2 bytes but got " + bytes.remaining());

            return bytes.getShort(bytes.position());
        }
    }

    public static class IntCodec extends TypeCodec<Integer> implements PrimitiveIntCodec {

        public static final IntCodec instance = new IntCodec();

        private IntCodec() {
            super(cint(), Integer.class);
        }

        @Override
        public Integer parse(String value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 32-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Integer value) {
            return Integer.toString(value);
        }

        @Override
        public ByteBuffer serialize(Integer value, ProtocolVersion protocolVersion) {
            return serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public ByteBuffer serializeNoBoxing(int value, ProtocolVersion protocolVersion) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(0, value);
            return bb;
        }

        @Override
        public Integer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return deserializeNoBoxing(bytes, protocolVersion);
        }

        @Override
        public int deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes.remaining() != 4)
                throw new InvalidTypeException("Invalid 32-bits integer value, expecting 4 bytes but got " + bytes.remaining());

            return bytes.getInt(bytes.position());
        }
    }

    public static class TimestampCodec extends TypeCodec<Date> {

        private static final String[] iso8601Patterns = new String[]{
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mmZ",
            "yyyy-MM-dd HH:mm:ssZ",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSSZ",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd'T'HH:mmZ",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "yyyy-MM-dd",
            "yyyy-MM-ddZ"
        };

        public static final TimestampCodec instance = new TimestampCodec();

        private static final Pattern IS_LONG_PATTERN = Pattern.compile("^-?\\d+$");

        private TimestampCodec() {
            super(timestamp(), Date.class);
        }

        /*
         * Copied and adapted from apache commons DateUtils.parseStrictly method (that is used Cassandra side
         * to parse date strings). It is copied here so as to not create a dependency on apache commons "just
         * for this".
         */
        private static Date parseDate(String str, final String[] parsePatterns) throws ParseException {
            SimpleDateFormat parser = new SimpleDateFormat();
            parser.setLenient(false);

            ParsePosition pos = new ParsePosition(0);
            for (String parsePattern : parsePatterns) {
                String pattern = parsePattern;

                parser.applyPattern(pattern);
                pos.setIndex(0);

                String str2 = str;
                Date date = parser.parse(str2, pos);
                if (date != null && pos.getIndex() == str2.length()) {
                    return date;
                }
            }
            throw new ParseException("Unable to parse the date: " + str, -1);
        }

        @Override
        public Date parse(String value) {
            // strip enclosing single quotes, if any
            if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'')
                value = value.substring(1, value.length() - 1);

            if (IS_LONG_PATTERN.matcher(value).matches()) {
                try {
                    return new Date(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
                }
            }

            try {
                return parseDate(value, iso8601Patterns);
            } catch (ParseException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }

        @Override
        public String format(Date value) {
            return Long.toString(value.getTime());
        }

        @Override
        public ByteBuffer serialize(Date value, ProtocolVersion protocolVersion) {
            return BigintCodec.instance.serializeNoBoxing(value.getTime(), protocolVersion);
        }

        @Override
        public Date deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return new Date(BigintCodec.instance.deserializeNoBoxing(bytes, protocolVersion));
        }
    }

    public static class DateCodec extends TypeCodec<LocalDate> {

        public static final DateCodec instance = new DateCodec();

        private static final Pattern IS_LONG_PATTERN = Pattern.compile("^-?\\d+$");
        private static final String pattern = "yyyy-MM-dd";
        private static final long MAX_LONG_VALUE = (1L << 32) - 1;
        private static final long EPOCH_AS_CQL_LONG = (1L << 31);

        private DateCodec() {
            super(date(), LocalDate.class);
        }

        @Override
        public LocalDate parse(String value) {

            // strip enclosing single quotes, if any
            // single quotes are optional for long literals, mandatory for date patterns
            if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'')
                value = value.substring(1, value.length() - 1);

            if (IS_LONG_PATTERN.matcher(value).matches()) {
                // In CQL, numeric DATE literals are longs between 0 and 2^32 - 1, with the epoch in the middle,
                // so parse it as a long and re-center at 0
                long cqlLong;
                try {
                    cqlLong = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
                }
                if (cqlLong < 0 || cqlLong > MAX_LONG_VALUE)
                    throw new InvalidTypeException(String.format("Numeric literals for DATE must be between 0 and %d (got %d)",
                        MAX_LONG_VALUE, cqlLong));

                int days = (int)(cqlLong - EPOCH_AS_CQL_LONG);

                return LocalDate.fromDaysSinceEpoch(days);
            }


            SimpleDateFormat parser = new SimpleDateFormat(pattern);
            parser.setLenient(false);
            parser.setTimeZone(TimeZone.getTimeZone("UTC"));

            ParsePosition pos = new ParsePosition(0);
            Date date = parser.parse(value, pos);
            if (date != null && pos.getIndex() == value.length()) {
                return LocalDate.fromMillisSinceEpoch(date.getTime());
            }

            throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
        }

        @Override
        public String format(LocalDate value) {
            return "'" + value.toString() + "'";
        }

        @Override
        public ByteBuffer serialize(LocalDate value, ProtocolVersion protocolVersion) {
            return IntCodec.instance.serializeNoBoxing(javaToProtocol(value.getDaysSinceEpoch()), protocolVersion);
        }

        @Override
        public LocalDate deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return LocalDate.fromDaysSinceEpoch(protocolToJava(IntCodec.instance.deserializeNoBoxing(bytes, protocolVersion)));
        }

        // The protocol encodes DATE as an _unsigned_ int with the epoch in the middle of the range (2^31).
        // We read this with ByteBuffer#getInt which expects a signed int, and we want epoch at 0.
        // These two methods handle the conversions.
        private static int protocolToJava(int p) {
            return p + Integer.MIN_VALUE; // this relies on overflow for "negative" values
        }

        private static int javaToProtocol(int j) {
            return j - Integer.MIN_VALUE;
        }
    }

    public static class TimeCodec extends LongCodec {

        private static final Pattern IS_LONG_PATTERN = Pattern.compile("^-?\\d+$");

        public static final TimeCodec instance = new TimeCodec();

        private TimeCodec() {
            super(time());
        }

        // Time specific parsing loosely based on java.sql.Timestamp
        private static Long parseTime(String s) throws IllegalArgumentException {
            String nanos_s;

            long hour;
            long minute;
            long second;
            long a_nanos = 0;

            String formatError = "Timestamp format must be hh:mm:ss[.fffffffff]";
            String zeros = "000000000";

            if (s == null)
                throw new java.lang.IllegalArgumentException(formatError);
            s = s.trim();

            // Parse the time
            int firstColon = s.indexOf(':');
            int secondColon = s.indexOf(':', firstColon + 1);

            // Convert the time; default missing nanos
            if (firstColon > 0 && secondColon > 0 && secondColon < s.length() - 1) {
                int period = s.indexOf('.', secondColon + 1);
                hour = Integer.parseInt(s.substring(0, firstColon));
                if (hour < 0 || hour >= 24)
                    throw new IllegalArgumentException("Hour out of bounds.");

                minute = Integer.parseInt(s.substring(firstColon + 1, secondColon));
                if (minute < 0 || minute >= 60)
                    throw new IllegalArgumentException("Minute out of bounds.");

                if (period > 0 && period < s.length() - 1) {
                    second = Integer.parseInt(s.substring(secondColon + 1, period));
                    if (second < 0 || second >= 60)
                        throw new IllegalArgumentException("Second out of bounds.");

                    nanos_s = s.substring(period + 1);
                    if (nanos_s.length() > 9)
                        throw new IllegalArgumentException(formatError);
                    if (!Character.isDigit(nanos_s.charAt(0)))
                        throw new IllegalArgumentException(formatError);
                    nanos_s = nanos_s + zeros.substring(0, 9 - nanos_s.length());
                    a_nanos = Integer.parseInt(nanos_s);
                } else if (period > 0)
                    throw new IllegalArgumentException(formatError);
                else {
                    second = Integer.parseInt(s.substring(secondColon + 1));
                    if (second < 0 || second >= 60)
                        throw new IllegalArgumentException("Second out of bounds.");
                }
            } else
                throw new IllegalArgumentException(formatError);

            long rawTime = 0;
            rawTime += TimeUnit.HOURS.toNanos(hour);
            rawTime += TimeUnit.MINUTES.toNanos(minute);
            rawTime += TimeUnit.SECONDS.toNanos(second);
            rawTime += a_nanos;
            return rawTime;
        }

        @Override
        public Long parse(String value) {
            // enclosing single quotes required, even for long literals
            if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
                throw new InvalidTypeException("time values must be enclosed by single quotes");
            value = value.substring(1, value.length() - 1);

            if (IS_LONG_PATTERN.matcher(value).matches()) {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse time value from \"%s\"", value));
                }
            }

            try {
                return parseTime(value);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse time value from \"%s\"", value));
            }
        }

        @Override
        public String format(Long value) {
            if (value == null)
                return "null";
            int nano = (int)(value % 1000);
            value -= nano;
            value /= 1000;
            int micro = (int)(value % 1000);
            value -= micro;
            value /= 1000;
            int milli = (int)(value % 1000);
            value -= milli;
            value /= 1000;
            int seconds = (int)(value % 60);
            value -= seconds;
            value /= 60;
            int minutes = (int)(value % 60);
            value -= minutes;
            value /= 60;
            int hours = (int)(value % 24);
            value -= hours;
            value /= 24;
            assert(value == 0);
            StringBuilder sb = new StringBuilder("'");
            leftPadZeros(hours, 2, sb);
            sb.append(":");
            leftPadZeros(minutes, 2, sb);
            sb.append(":");
            leftPadZeros(seconds, 2, sb);
            sb.append(".");
            leftPadZeros(milli, 3, sb);
            leftPadZeros(micro, 3, sb);
            leftPadZeros(nano, 3, sb);
            sb.append("'");
            return sb.toString();
        }

        private static void leftPadZeros(int value, int digits, StringBuilder sb) {
            for (int i = 1; i < digits; ++i) {
                if (value < Math.pow(10, i))
                    sb.append("0");
            }
            sb.append(value);
        }

    }

    public static abstract class AbstractUUIDCodec extends TypeCodec<UUID> {

        private AbstractUUIDCodec(DataType cqlType) {
            super(cqlType, UUID.class);
        }

        @Override
        public UUID parse(String value) {
            try {
                return UUID.fromString(value);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse UUID value from \"%s\"", value));
            }
        }

        @Override
        public String format(UUID value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(UUID value, ProtocolVersion protocolVersion) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(0, value.getMostSignificantBits());
            bb.putLong(8, value.getLeastSignificantBits());
            return bb;
        }

        @Override
        public UUID deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return new UUID(bytes.getLong(bytes.position()), bytes.getLong(bytes.position() + 8));
        }
    }

    public static class UUIDCodec extends AbstractUUIDCodec {

        public static final UUIDCodec instance = new UUIDCodec();

        private UUIDCodec() {
            super(uuid());
        }

    }

    public static class TimeUUIDCodec extends AbstractUUIDCodec {

        public static final TimeUUIDCodec instance = new TimeUUIDCodec();

        private TimeUUIDCodec() {
            super(timeuuid());
        }

        @Override
        public String format(UUID value) {
            if (value.version() != 1)
                throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", value));
            return super.format(value);
        }

        @Override
        public ByteBuffer serialize(UUID value, ProtocolVersion protocolVersion) {
            if (value.version() != 1)
                throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", value));
            return super.serialize(value, protocolVersion);
        }
    }

    public static class VarintCodec extends TypeCodec<BigInteger> {

        public static final VarintCodec instance = new VarintCodec();

        private VarintCodec() {
            super(varint(), BigInteger.class);
        }

        @Override
        public BigInteger parse(String value) {
            try {
                return new BigInteger(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse varint value from \"%s\"", value));
            }
        }

        @Override
        public String format(BigInteger value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(BigInteger value, ProtocolVersion protocolVersion) {
            return ByteBuffer.wrap(value.toByteArray());
        }

        @Override
        public BigInteger deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return new BigInteger(Bytes.getArray(bytes));
        }
    }

    public abstract static class CollectionCodec<E, C extends Collection<E>> extends TypeCodec<C> {

        protected final TypeCodec<E> eltCodec;

        public CollectionCodec(CollectionType cqlType, TypeToken<C> javaType, TypeCodec<E> eltCodec) {
            super(cqlType, javaType);
            this.eltCodec = eltCodec;
        }

        @Override
        public ByteBuffer serialize(C value, ProtocolVersion protocolVersion) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
            for (E elt : value) {
                if (elt == null) {
                    throw new NullPointerException("Collection elements cannot be null");
                }
                ByteBuffer bb;
                try {
                    bb = eltCodec.serialize(elt, protocolVersion);
                } catch (ClassCastException e) {
                    throw new InvalidTypeException(
                        String.format("Invalid type for %s element, expecting %s but got %s",
                            cqlType, eltCodec.getJavaType(), elt.getClass()), e);
                }
                bbs.add(bb);
            }
            return CodecUtils.pack(bbs, value.size(), protocolVersion);
        }

        @Override
        public C deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = CodecUtils.readCollectionSize(input, protocolVersion);
                C coll = newInstance(n);
                for (int i = 0; i < n; i++) {
                    ByteBuffer databb = CodecUtils.readCollectionValue(input, protocolVersion);
                    coll.add(eltCodec.deserialize(databb, protocolVersion));
                }
                return unmodifiable(coll);
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize list");
            }
        }

        @Override
        public String format(C value) {
            StringBuilder sb = new StringBuilder();
            sb.append(getOpeningChar());
            int i = 0;
            for (E v : value) {
                if (i++ != 0)
                    sb.append(",");
                sb.append(eltCodec.format(v));
            }
            sb.append(getClosingChar());
            return sb.toString();
        }

        @Override
        public C parse(String value) {
            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != getOpeningChar())
                throw new InvalidTypeException(String.format("cannot parse list value from \"%s\", at character %d expecting '[' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == getClosingChar())
                return newInstance(0);

            C l = newInstance(10);
            while (idx < value.length()) {
                int n;
                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                l.add(eltCodec.parse(value.substring(idx, n)));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == getClosingChar())
                    return l;
                if (value.charAt(idx++) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(String.format("Malformed list value \"%s\", missing closing ']'", value));
        }

        @Override
        public boolean accepts(Object value) {
            if (getJavaType().getRawType().isAssignableFrom(value.getClass())) {
                // runtime type ok, now check element type
                Collection<?> list = (Collection<?>)value;
                if (list.isEmpty())
                    return true;
                Object elt = list.iterator().next();
                return eltCodec.accepts(elt);
            }
            return false;
        }

        protected abstract C newInstance(int capacity);

        protected abstract C unmodifiable(C coll);

        protected abstract char getOpeningChar();

        protected abstract char getClosingChar();

    }

    public static class ListCodec<T> extends CollectionCodec<T, List<T>> {

        public ListCodec(TypeCodec<T> eltCodec) {
            super(list(eltCodec.getCqlType()), listOf(eltCodec.getJavaType()), eltCodec);
        }

        @Override
        protected List<T> newInstance(int capacity) {
            return new ArrayList<T>(capacity);
        }

        @Override
        protected List<T> unmodifiable(List<T> coll) {
            return Collections.unmodifiableList(coll);
        }

        @Override
        protected char getOpeningChar() {
            return '[';
        }

        @Override
        protected char getClosingChar() {
            return ']';
        }

    }

    public static class SetCodec<T> extends CollectionCodec<T, Set<T>> {

        public SetCodec(TypeCodec<T> eltCodec) {
            super(set(eltCodec.cqlType), setOf(eltCodec.getJavaType()), eltCodec);
        }

        protected Set<T> newInstance(int capacity) {
            return new LinkedHashSet<T>(capacity);
        }

        @Override
        protected Set<T> unmodifiable(Set<T> coll) {
            return Collections.unmodifiableSet(coll);
        }

        protected char getOpeningChar() {
            return '{';
        }

        protected char getClosingChar() {
            return '}';
        }
    }

    public static class MapCodec<K, V> extends TypeCodec<Map<K, V>> {

        private final TypeCodec<K> keyCodec;

        private final TypeCodec<V> valueCodec;

        public MapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec) {
            super(map(keyCodec.getCqlType(), valueCodec.getCqlType()), mapOf(keyCodec.getJavaType(), valueCodec.getJavaType()));
            this.keyCodec = keyCodec;
            this.valueCodec = valueCodec;
        }

        @Override
        public boolean accepts(Object value) {
            if (value instanceof Map) {
                // runtime type ok, now check key and value types
                Map<?, ?> map = (Map<?, ?>)value;
                if (map.isEmpty())
                    return true;
                Map.Entry<?, ?> entry = map.entrySet().iterator().next();
                return keyCodec.accepts(entry.getKey()) && valueCodec.accepts(entry.getValue());
            }
            return false;
        }

        @Override
        public Map<K, V> parse(String value) {
            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '{')
                throw new InvalidTypeException(String.format("cannot parse map value from \"%s\", at character %d expecting '{' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == '}')
                return Collections.emptyMap();

            Map<K, V> m = new HashMap<K, V>();
            while (idx < value.length()) {
                int n;
                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse map value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                K k = keyCodec.parse(value.substring(idx, n));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx++) != ':')
                    throw new InvalidTypeException(String.format("Cannot parse map value from \"%s\", at character %d expecting ':' but got '%c'", value, idx, value.charAt(idx)));
                idx = ParseUtils.skipSpaces(value, idx);

                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse map value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                V v = valueCodec.parse(value.substring(idx, n));
                idx = n;

                m.put(k, v);

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == '}')
                    return m;
                if (value.charAt(idx++) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse map value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));

                idx = ParseUtils.skipSpaces(value, idx);

            }
            throw new InvalidTypeException(String.format("Malformed map value \"%s\", missing closing '}'", value));
        }

        @Override
        public String format(Map<K, V> value) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            int i = 0;
            for (Map.Entry<K, V> e : value.entrySet()) {
                if (i++ != 0)
                    sb.append(",");
                sb.append(keyCodec.format(e.getKey()));
                sb.append(":");
                sb.append(valueCodec.format(e.getValue()));
            }
            sb.append("}");
            return sb.toString();
        }

        @Override
        public ByteBuffer serialize(Map<K, V> value, ProtocolVersion protocolVersion) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(2 * value.size());
            for (Map.Entry<K, V> entry : value.entrySet()) {
                ByteBuffer bbk;
                K key = entry.getKey();
                if (key == null) {
                    throw new NullPointerException("Map keys cannot be null");
                }
                try {
                    bbk = keyCodec.serialize(key, protocolVersion);
                } catch (ClassCastException e) {
                    throw new InvalidTypeException(String.format("Invalid type for map key, expecting % but got %s", keyCodec.getJavaType(), key.getClass()), e);
                }
                ByteBuffer bbv;
                V v = entry.getValue();
                if (v == null) {
                    throw new NullPointerException("Map values cannot be null");
                }
                try {
                    bbv = valueCodec.serialize(v, protocolVersion);
                } catch (ClassCastException e) {
                    throw new InvalidTypeException(String.format("Invalid type for map value, expecting % but got %s", valueCodec.getJavaType(), v.getClass()), e);
                }
                bbs.add(bbk);
                bbs.add(bbv);
            }
            return CodecUtils.pack(bbs, value.size(), protocolVersion);
        }

        @Override
        public Map<K, V> deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = CodecUtils.readCollectionSize(input, protocolVersion);
                Map<K, V> m = new LinkedHashMap<K, V>(n);
                for (int i = 0; i < n; i++) {
                    ByteBuffer kbb = CodecUtils.readCollectionValue(input, protocolVersion);
                    ByteBuffer vbb = CodecUtils.readCollectionValue(input, protocolVersion);
                    m.put(keyCodec.deserialize(kbb, protocolVersion), valueCodec.deserialize(vbb, protocolVersion));
                }
                return Collections.unmodifiableMap(m);
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize a map");
            }
        }
    }

    public static class UDTCodec extends TypeCodec<UDTValue> {

        private final UserType definition;

        public UDTCodec(UserType definition) {
            super(definition, UDTValue.class);
            this.definition = definition;
        }

        @Override
        public boolean accepts(Object value) {
            if(value instanceof UDTValue) {
                return ((UDTValue) value).getType().equals(definition);
            }
            return false;
        }

        @Override
        public UDTValue parse(String value) {
            UDTValue v = definition.newValue();

            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '{')
                throw new InvalidTypeException(String.format("Cannot parse UDT value from \"%s\", at character %d expecting '{' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == '}')
                return v;

            while (idx < value.length()) {

                int n;
                try {
                    n = ParseUtils.skipCQLId(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse UDT value from \"%s\", cannot parse a CQL identifier at character %d", value, idx), e);
                }
                String name = value.substring(idx, n);
                idx = n;

                if (!definition.contains(name))
                    throw new InvalidTypeException(String.format("Unknown field %s in value \"%s\"", name, value));

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx++) != ':')
                    throw new InvalidTypeException(String.format("Cannot parse UDT value from \"%s\", at character %d expecting ':' but got '%c'", value, idx, value.charAt(idx)));
                idx = ParseUtils.skipSpaces(value, idx);

                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse UDT value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                DataType dt = definition.getFieldType(name);
                TypeCodec<Object> codec = definition.getCodecRegistry().codecFor(dt);
                v.setObject(name, codec.parse(value.substring(idx, n)));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == '}')
                    return v;
                if (value.charAt(idx) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse UDT value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));
                ++idx; // skip ','

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(String.format("Malformed UDT value \"%s\", missing closing '}'", value));
        }

        @Override
        public String format(UDTValue value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(UDTValue value, ProtocolVersion protocolVersion) {
            int size = 0;
            for (ByteBuffer v : value.values)
                size += 4 + (v == null ? 0 : v.remaining());

            ByteBuffer result = ByteBuffer.allocate(size);
            for (ByteBuffer bb : value.values) {
                if (bb == null) {
                    result.putInt(-1);
                } else {
                    result.putInt(bb.remaining());
                    result.put(bb.duplicate());
                }
            }
            return (ByteBuffer)result.flip();
        }

        @Override
        public UDTValue deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            ByteBuffer input = bytes.duplicate();
            UDTValue value = definition.newValue();

            int i = 0;
            while (input.hasRemaining() && i < value.values.length) {
                int n = input.getInt();
                value.values[i++] = n < 0 ? null : CodecUtils.readBytes(input, n);
            }
            return value;
        }
    }

    public static class TupleCodec extends TypeCodec<TupleValue> {

        private final TupleType definition;

        public TupleCodec(TupleType definition) {
            super(definition, TupleValue.class);
            this.definition = definition;
        }

        @Override
        public boolean accepts(DataType cqlType) {
            // a tuple codec should accept tuple values fo a different type,
            // provided that the latter is contained in this codec's type.
            return cqlType instanceof TupleType && definition.contains((TupleType)cqlType);
        }

        @Override
        public boolean accepts(Object value) {
            // a tuple codec should accept tuple values fo a different type,
            // provided that the latter is contained in this codec's type.
            return value instanceof TupleValue && definition.contains(((TupleValue) value).getType());
        }

        @Override
        public TupleValue parse(String value) {
            TupleValue v = definition.newValue();

            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '(')
                throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", at character %d expecting '(' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == ')')
                return v;

            int i = 0;
            while (idx < value.length()) {
                int n;
                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                DataType dt = definition.getComponentTypes().get(i);
                TypeCodec<Object> codec = definition.getCodecRegistry().codecFor(dt);
                v.setObject(i, codec.parse(value.substring(idx, n)));
                idx = n;
                i += 1;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == ')')
                    return v;
                if (value.charAt(idx) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));
                ++idx; // skip ','

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(String.format("Malformed tuple value \"%s\", missing closing ')'", value));
        }

        @Override
        public String format(TupleValue value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(TupleValue value, ProtocolVersion protocolVersion) {
            int size = 0;
            for (ByteBuffer v : value.values)
                size += 4 + (v == null ? 0 : v.remaining());

            ByteBuffer result = ByteBuffer.allocate(size);
            for (ByteBuffer bb : value.values) {
                if (bb == null) {
                    result.putInt(-1);
                } else {
                    result.putInt(bb.remaining());
                    result.put(bb.duplicate());
                }
            }
            return (ByteBuffer)result.flip();
        }

        @Override
        public TupleValue deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            ByteBuffer input = bytes.duplicate();
            TupleValue value = definition.newValue();

            int i = 0;
            while (input.hasRemaining() && i < value.values.length) {
                int n = input.getInt();
                value.values[i++] = n < 0 ? null : CodecUtils.readBytes(input, n);
            }
            return value;
        }
    }

    /**
     * An abstract TypeCodec that actually stores objects as serialized strings.
     * This can serve as a base for codecs dealing with XML or JSON formats.
     *
     * @param <T> The Java type this codec serializes from and deserializes to.
     */
    public abstract static class ParsingTypeCodec<T> extends TypeCodec<T> {

        private final StringCodec codec;

        public ParsingTypeCodec(Class<T> javaType) {
            this(TypeToken.of(javaType));
        }

        public ParsingTypeCodec(TypeToken<T> javaType) {
            this(VarcharCodec.instance, javaType);
        }

        public ParsingTypeCodec(StringCodec codec, Class<T> javaType) {
            this(codec, TypeToken.of(javaType));
        }

        public ParsingTypeCodec(StringCodec codec, TypeToken<T> javaType) {
            super(codec.getCqlType(), javaType);
            this.codec = codec;
        }

        @Override
        public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return codec.serialize(format(value), protocolVersion);
        }

        @Override
        public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return parse(codec.deserialize(bytes, protocolVersion));
        }

    }

    /**
     * An abstract TypeCodec that maps a Java Pojo to another Java object
     * that can in turn be serialized into a CQL type.
     * This can serve as a base for libraries dealing with Pojo mappings.
     *
     * @param <T> The outer Java type
     * @param <U> The inner Java type
     */
    public abstract static class MappingCodec<T, U> extends TypeCodec<T> {

        protected final TypeCodec<U> innerCodec;

        public MappingCodec(TypeCodec<U> innerCodec, Class<T> javaType) {
            this(innerCodec, TypeToken.of(javaType));
        }

        public MappingCodec(TypeCodec<U> innerCodec, TypeToken<T> javaType) {
            super(innerCodec.getCqlType(), javaType);
            this.innerCodec = innerCodec;
        }

        @Override
        public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return innerCodec.serialize(serialize(value), protocolVersion);
        }

        @Override
        public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return deserialize(innerCodec.deserialize(bytes, protocolVersion));
        }

        @Override
        public T parse(String value) throws InvalidTypeException {
            return deserialize(innerCodec.parse(value));
        }

        @Override
        public String format(T value) throws InvalidTypeException {
            return innerCodec.format(serialize(value));
        }

        protected abstract T deserialize(U value);

        protected abstract U serialize(T value);

    }

    /**
     * A codec that serializes Enum instances as CQL {@link DataType#varchar() varchar}s representing the Enum instance name
     * as returned by {@link Enum#name()}.
     *
     * @param <E> The Enum class this codec serializes from and deserializes to.
     */
    public static class EnumCodec<E extends Enum<E>> extends ParsingTypeCodec<E> {

        private final Class<E> enumClass;

        public EnumCodec(Class<E> enumClass) {
            super(enumClass);
            this.enumClass = enumClass;
        }

        @Override
        public E parse(String value) throws InvalidTypeException {
            return Enum.valueOf(enumClass, value);
        }

        @Override
        public String format(E value) throws InvalidTypeException {
            return value.name();
        }
    }

}
