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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.reflect.TypeToken;

import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.CodecUtils.mapOf;
import static com.datastax.driver.core.CodecUtils.setOf;
import static com.datastax.driver.core.DataType.Name.*;

abstract class AbstractGettableByIndexData implements GettableByIndexData {

    protected final ProtocolVersion protocolVersion;

    protected AbstractGettableByIndexData(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    /**
     * Returns the type for the value at index {@code i}.
     *
     * @param i the index of the type to fetch.
     * @return the type of the value at index {@code i}.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract DataType getType(int i);

    /**
     * Returns the name corresponding to the value at index {@code i}.
     *
     * @param i the index of the name to fetch.
     * @return the name corresponding to the value at index {@code i}.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract String getName(int i);

    /**
     * Returns the qualified name corresponding to the value at index {@code i}.
     *
     * @param i the index of the name to fetch.
     * @return the qualified name corresponding to the value at index {@code i}.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract String getQualifiedName(int i);

    /**
     * Returns the value at index {@code i}.
     *
     * @param i the index to fetch.
     * @return the value at index {@code i}.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract ByteBuffer getValue(int i);

    protected abstract CodecRegistry getCodecRegistry();

    protected <T> TypeCodec<T> codecFor(int i, Class<T> javaClass){
        return getCodecRegistry().codecFor(getType(i), TypeToken.of(javaClass), getQualifiedName(i));
    }

    protected <T> TypeCodec<T> codecFor(int i, TypeToken<T> javaType) {
        return getCodecRegistry().codecFor(getType(i), javaType, getQualifiedName(i));
    }

    protected <T> TypeCodec<T> codecFor(int i) {
        return getCodecRegistry().codecFor(getType(i), getQualifiedName(i));
    }

    protected <T> TypeCodec<T> codecFor(int i, T value) {
        return getCodecRegistry().codecFor(getType(i), value, getQualifiedName(i));
    }

    // Note: we avoid having a vararg method to avoid the array allocation that comes with it.
    protected void checkType(int i, DataType.Name name) {
        DataType defined = getType(i);
        if (name != defined.getName())
            throw new InvalidTypeException(String.format("Value %s is of type %s", getName(i), defined));
    }

    protected void checkType(int i, DataType.Name name1, DataType.Name name2) {
        DataType defined = getType(i);
        if (name1 != defined.getName() && name2 != defined.getName())
            throw new InvalidTypeException(String.format("Value %s is of type %s", getName(i), defined));
    }

    protected void checkType(int i, DataType.Name name1, DataType.Name name2, DataType.Name name3) {
        DataType defined = getType(i);
        if (name1 != defined.getName() && name2 != defined.getName() && name3 != defined.getName())
            throw new InvalidTypeException(String.format("Value %s is of type %s", getName(i), defined));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull(int i) {
        return getValue(i) == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBool(int i) {
        checkType(i, BOOLEAN);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return false;

        TypeCodec<Boolean> codec = codecFor(i, Boolean.class);
        if(codec instanceof TypeCodec.PrimitiveBooleanCodec)
            return ((TypeCodec.PrimitiveBooleanCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else
            return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(int i) {
        checkType(i, TINYINT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0;

        TypeCodec<Byte> codec = codecFor(i, Byte.class);
        if(codec instanceof TypeCodec.PrimitiveByteCodec)
            return ((TypeCodec.PrimitiveByteCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else
            return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getShort(int i) {
        checkType(i, SMALLINT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0;

        TypeCodec<Short> codec = codecFor(i, Short.class);
        if(codec instanceof TypeCodec.PrimitiveShortCodec)
            return ((TypeCodec.PrimitiveShortCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else
            return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(int i) {
        checkType(i, INT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0;

        TypeCodec<Integer> codec = codecFor(i, Integer.class);
        if(codec instanceof TypeCodec.PrimitiveIntCodec)
            return ((TypeCodec.PrimitiveIntCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else
            return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(int i) {
        checkType(i, BIGINT, COUNTER);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0L;

        TypeCodec<Long> codec = codecFor(i, Long.class);
        if(codec instanceof TypeCodec.PrimitiveLongCodec)
            return ((TypeCodec.PrimitiveLongCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else
            return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getTimestamp(int i) {
        checkType(i, TIMESTAMP);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, Date.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate getDate(int i) {
        checkType(i, DATE);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, LocalDate.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTime(int i) {
        checkType(i, TIME);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0L;

        TypeCodec<Long> codec = codecFor(i, Long.class);
        if(codec instanceof TypeCodec.PrimitiveLongCodec)
            return ((TypeCodec.PrimitiveLongCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else
            return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(int i) {
        checkType(i, FLOAT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0.0f;

        TypeCodec<Float> codec = codecFor(i, Float.class);
        if(codec instanceof TypeCodec.PrimitiveFloatCodec)
            return ((TypeCodec.PrimitiveFloatCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else
            return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(int i) {
        checkType(i, DOUBLE);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0.0;

        TypeCodec<Double> codec = codecFor(i, Double.class);
        if(codec instanceof TypeCodec.PrimitiveDoubleCodec)
            return ((TypeCodec.PrimitiveDoubleCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else
            return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return value.duplicate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytes(int i) {
        checkType(i, BLOB);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, ByteBuffer.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(int i) {
        checkType(i, VARCHAR, TEXT, ASCII);

        ByteBuffer value = getValue(i);
        if (value == null)
            return null;
        if(value.remaining() == 0)
            return "";

        return codecFor(i, String.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger getVarint(int i) {
        checkType(i, VARINT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, BigInteger.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getDecimal(int i) {
        checkType(i, DECIMAL);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, BigDecimal.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getUUID(int i) {
        checkType(i, UUID, TIMEUUID);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, UUID.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getInet(int i) {
        checkType(i, INET);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, InetAddress.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        return getList(i, TypeToken.of(elementsClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, TypeToken<T> elementsType) {
        checkType(i, LIST);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return Collections.emptyList();

        TypeToken<List<T>> javaType = listOf(elementsType);
        return codecFor(i, javaType).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        return getSet(i, TypeToken.of(elementsClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
        checkType(i, SET);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return Collections.emptySet();

        TypeToken<Set<T>> javaType = setOf(elementsType);
        return codecFor(i, javaType).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        return getMap(i, TypeToken.of(keysClass), TypeToken.of(valuesClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        checkType(i, MAP);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return Collections.emptyMap();

        TypeToken<Map<K, V>> javaType = mapOf(keysType, valuesType);
        return codecFor(i, javaType).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public UDTValue getUDTValue(int i) {
        checkType(i, UDT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, UDTValue.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TupleValue getTupleValue(int i) {
        checkType(i, TUPLE);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, TupleValue.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(int i) {
        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i).deserialize(value, protocolVersion);
    }

    @Override
    public <T> T getObject(int i, Class<T> targetClass) {
        return getObject(i, TypeToken.of(targetClass));
    }

    @Override
    public <T> T getObject(int i, TypeToken<T> targetType) {
        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;
        return codecFor(i, targetType).deserialize(value, protocolVersion);
    }

}
