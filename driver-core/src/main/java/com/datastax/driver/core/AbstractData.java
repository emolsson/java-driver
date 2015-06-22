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

import com.google.common.reflect.TypeToken;

import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.CodecUtils.mapOf;
import static com.datastax.driver.core.CodecUtils.setOf;
import static com.datastax.driver.core.DataType.Name.*;

// We don't want to expose this one: it's less useful externally and it's a bit ugly to expose anyway (but it's convenient).
abstract class AbstractData<T extends SettableData<T>> extends AbstractGettableData implements SettableData<T> {

    final T wrapped;
    final ByteBuffer[] values;

    // Ugly, we could probably clean that: it is currently needed however because we sometimes
    // want wrapped to be 'this' (UDTValue), and sometimes some other object (in BoundStatement).
    @SuppressWarnings("unchecked")
    protected AbstractData(ProtocolVersion protocolVersion, int size) {
        super(protocolVersion);
        this.wrapped = (T)this;
        this.values = new ByteBuffer[size];
    }

    protected AbstractData(ProtocolVersion protocolVersion, T wrapped, int size) {
        this(protocolVersion, wrapped, new ByteBuffer[size]);
    }

    protected AbstractData(ProtocolVersion protocolVersion, T wrapped, ByteBuffer[] values) {
        super(protocolVersion);
        this.wrapped = wrapped;
        this.values = values;
    }

    protected abstract int[] getAllIndexesOf(String name);

    private T setValue(int i, ByteBuffer value) {
        values[i] = value;
        return wrapped;
    }

    protected ByteBuffer getValue(int i) {
        return values[i];
    }

    protected int getIndexOf(String name) {
        return getAllIndexesOf(name)[0];
    }

    public T setBool(int i, boolean v) {
        checkType(i, BOOLEAN);
        TypeCodec<Boolean> codec = codecFor(i, Boolean.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveBooleanCodec)
            bb = ((TypeCodec.PrimitiveBooleanCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setBool(String name, boolean v) {
        for (int i : getAllIndexesOf(name)) {
            setBool(i, v);
        }
        return wrapped;
    }

    public T setByte(int i, byte v) {
        checkType(i, TINYINT);
        TypeCodec<Byte> codec = codecFor(i, Byte.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveByteCodec)
            bb = ((TypeCodec.PrimitiveByteCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setByte(String name, byte v) {
        for (int i : getAllIndexesOf(name)) {
            setByte(i, v);
        }
        return wrapped;
    }

    public T setShort(int i, short v) {
        checkType(i, SMALLINT);
        TypeCodec<Short> codec = codecFor(i, Short.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveShortCodec)
            bb = ((TypeCodec.PrimitiveShortCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setShort(String name, short v) {
        for (int i : getAllIndexesOf(name)) {
            setShort(i, v);
        }
        return wrapped;
    }

    public T setInt(int i, int v) {
        checkType(i, INT);
        TypeCodec<Integer> codec = codecFor(i, Integer.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveIntCodec)
            bb = ((TypeCodec.PrimitiveIntCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setInt(String name, int v) {
        for (int i : getAllIndexesOf(name)) {
            setInt(i, v);
        }
        return wrapped;
    }

    public T setLong(int i, long v) {
        checkType(i, BIGINT, COUNTER);
        TypeCodec<Long> codec = codecFor(i, Long.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveLongCodec)
            bb = ((TypeCodec.PrimitiveLongCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setLong(String name, long v) {
        for (int i : getAllIndexesOf(name)) {
            setLong(i, v);
        }
        return wrapped;
    }

    public T setTimestamp(int i, Date v) {
        checkType(i, TIMESTAMP);
        return setValue(i, v == null ? null : codecFor(i, Date.class).serialize(v, protocolVersion));
    }

    public T setTimestamp(String name, Date v) {
        for (int i : getAllIndexesOf(name)) {
            setTimestamp(i, v);
        }
        return wrapped;
    }

    public T setDate(int i, LocalDate v) {
        checkType(i, DATE);
        return setValue(i, v == null ? null : codecFor(i, LocalDate.class).serialize(v, protocolVersion));
    }

    public T setDate(String name, LocalDate v) {
        for (int i : getAllIndexesOf(name)) {
            setDate(i, v);
        }
        return wrapped;
    }

    public T setTime(int i, long v) {
        checkType(i, TIME);
        TypeCodec<Long> codec = codecFor(i, Long.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveLongCodec)
            bb = ((TypeCodec.PrimitiveLongCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setTime(String name, long v) {
        for (int i : getAllIndexesOf(name)) {
            setTime(i, v);
        }
        return wrapped;
    }

    public T setFloat(int i, float v) {
        checkType(i, FLOAT);
        TypeCodec<Float> codec = codecFor(i, Float.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveFloatCodec)
            bb = ((TypeCodec.PrimitiveFloatCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setFloat(String name, float v) {
        for (int i : getAllIndexesOf(name)) {
            setFloat(i, v);
        }
        return wrapped;
    }

    public T setDouble(int i, double v) {
        checkType(i, DOUBLE);
        TypeCodec<Double> codec = codecFor(i, Double.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveDoubleCodec)
            bb = ((TypeCodec.PrimitiveDoubleCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setDouble(String name, double v) {
        for (int i : getAllIndexesOf(name)) {
            setDouble(i, v);
        }
        return wrapped;
    }

    public T setString(int i, String v) {
        checkType(i, VARCHAR, TEXT, ASCII);
        return setValue(i, v == null ? null : codecFor(i, String.class).serialize(v, protocolVersion));
    }

    public T setString(String name, String v) {
        for (int i : getAllIndexesOf(name)) {
            setString(i, v);
        }
        return wrapped;
    }

    public T setBytes(int i, ByteBuffer v) {
        checkType(i, BLOB);
        return setValue(i, v == null ? null : codecFor(i, ByteBuffer.class).serialize(v, protocolVersion));
    }

    public T setBytes(String name, ByteBuffer v) {
        for (int i : getAllIndexesOf(name)) {
            setBytes(i, v);
        }
        return wrapped;
    }

    public T setBytesUnsafe(int i, ByteBuffer v) {
        return setValue(i, v == null ? null : v.duplicate());
    }

    public T setBytesUnsafe(String name, ByteBuffer v) {
        ByteBuffer value = v == null ? null : v.duplicate();
        for (int i : getAllIndexesOf(name)) {
            setValue(i, value);
        }
        return wrapped;
    }

    public T setVarint(int i, BigInteger v) {
        checkType(i, VARINT);
        return setValue(i, v == null ? null : codecFor(i, BigInteger.class).serialize(v, protocolVersion));
    }

    public T setVarint(String name, BigInteger v) {
        for (int i : getAllIndexesOf(name)) {
            setVarint(i, v);
        }
        return wrapped;
    }

    public T setDecimal(int i, BigDecimal v) {
        checkType(i, DECIMAL);
        return setValue(i, v == null ? null : codecFor(i, BigDecimal.class).serialize(v, protocolVersion));
    }

    public T setDecimal(String name, BigDecimal v) {
        for (int i : getAllIndexesOf(name)) {
            setDecimal(i, v);
        }
        return wrapped;
    }

    public T setUUID(int i, UUID v) {
        checkType(i, UUID, TIMEUUID);
        return setValue(i, v == null ? null : codecFor(i, UUID.class).serialize(v, protocolVersion));
    }

    public T setUUID(String name, UUID v) {
        for (int i : getAllIndexesOf(name)) {
            setUUID(i, v);
        }
        return wrapped;
    }

    public T setInet(int i, InetAddress v) {
        checkType(i, INET);
        return setValue(i, v == null ? null : codecFor(i, InetAddress.class).serialize(v, protocolVersion));
    }

    public T setInet(String name, InetAddress v) {
        for (int i : getAllIndexesOf(name)) {
            setInet(i, v);
        }
        return wrapped;
    }

    // setToken is package-private because we only want to expose it in BoundStatement
    T setToken(int i, Token v) {
        if (v == null)
            throw new NullPointerException(String.format("Cannot set a null token for column %s", getName(i)));
        checkType(i, v.getType().getName());
        // Bypass CodecRegistry when serializing tokens
        return setValue(i, v.serialize(protocolVersion));
    }

    T setToken(String name, Token v) {
        for (int i : getAllIndexesOf(name)) {
            setToken(i, v);
        }
        return wrapped;
    }

    @SuppressWarnings("unchecked")
    public <E> T setList(int i, List<E> v) {
        checkType(i, LIST);
        if (v == null)
            return setValue(i, null);
        TypeToken<E> eltType;
        if(v.isEmpty()) {
            eltType = (TypeToken<E>)getCodecRegistry().codecFor(getType(i).getTypeArguments().get(0)).getJavaType();
        } else {
            eltType = getCodecRegistry().codecFor(v.get(0)).javaType;
        }
        return setValue(i, codecFor(i, listOf(eltType)).serialize(v, protocolVersion));
    }

    public <E> T setList(String name, List<E> v) {
        for (int i : getAllIndexesOf(name)) {
            setList(i, v);
        }
        return wrapped;
    }

    @SuppressWarnings("unchecked")
    public <K, V> T setMap(int i, Map<K, V> v) {
        checkType(i, MAP);
        if (v == null)
            return setValue(i, null);
        DataType cqlType = getType(i);
        TypeToken<K> keysType;
        TypeToken<V> valuesType;
        if(v.isEmpty()) {
            keysType = (TypeToken<K>)  getCodecRegistry().codecFor(cqlType.getTypeArguments().get(0)).getJavaType();
            valuesType = (TypeToken<V>)  getCodecRegistry().codecFor(cqlType.getTypeArguments().get(1)).getJavaType();
        } else {
            Map.Entry<K, V> entry = v.entrySet().iterator().next();
            keysType = getCodecRegistry().codecFor(entry.getKey()).getJavaType();
            valuesType = getCodecRegistry().codecFor(entry.getValue()).getJavaType();
        }
        return setValue(i, codecFor(i, mapOf(keysType, valuesType)).serialize(v, protocolVersion));
    }

    public <K, V> T setMap(String name, Map<K, V> v) {
        for (int i : getAllIndexesOf(name)) {
            setMap(i, v);
        }
        return wrapped;
    }

    @SuppressWarnings("unchecked")
    public <E> T setSet(int i, Set<E> v) {
        checkType(i, SET);
        if (v == null)
            return setValue(i, null);
        TypeToken<E> eltType;
        if(v.isEmpty()) {
            eltType = (TypeToken<E>)getCodecRegistry().codecFor(getType(i).getTypeArguments().get(0)).getJavaType();
        } else {
            eltType = getCodecRegistry().codecFor(v.iterator().next()).getJavaType();
        }
        return setValue(i, codecFor(i, setOf(eltType)).serialize(v, protocolVersion));
    }

    public <E> T setSet(String name, Set<E> v) {
        for (int i : getAllIndexesOf(name)) {
            setSet(i, v);
        }
        return wrapped;
    }

    public T setUDTValue(int i, UDTValue v) {
        checkType(i, UDT);
        return setValue(i, v == null ? null : codecFor(i, UDTValue.class).serialize(v, protocolVersion));
    }

    public T setUDTValue(String name, UDTValue v) {
        for (int i : getAllIndexesOf(name)) {
            setUDTValue(i, v);
        }
        return wrapped;
    }

    public T setTupleValue(int i, TupleValue v) {
        checkType(i, TUPLE);
        return setValue(i, v == null ? null : codecFor(i, TupleValue.class).serialize(v, protocolVersion));
    }

    public T setTupleValue(String name, TupleValue v) {
        for (int i : getAllIndexesOf(name)) {
            setTupleValue(i, v);
        }
        return wrapped;
    }

    @Override
    public <V> T setObject(int i, V v) {
        return setValue(i, v == null ? null : codecFor(i).serialize(v, protocolVersion));
    }

    @Override
    public <V> T setObject(String name, V v) {
        for (int i : getAllIndexesOf(name)) {
            setObject(i, v);
        }
        return wrapped;
    }

    @Override
    public <V> T setObject(int i, V v, Class<V> targetClass) {
        return setValue(i, v == null ? null : codecFor(i, targetClass).serialize(v, protocolVersion));
    }

    @Override
    public <V> T setObject(String name, V v, Class<V> targetClass) {
        for (int i : getAllIndexesOf(name)) {
            setObject(i, v, targetClass);
        }
        return wrapped;
    }

    @Override
    public <V> T setObject(int i, V v, TypeToken<V> targetType) {
        return setValue(i, v == null ? null : codecFor(i, targetType).serialize(v, protocolVersion));
    }

    @Override
    public <V> T setObject(String name, V v, TypeToken<V> targetType) {
        for (int i : getAllIndexesOf(name)) {
            setObject(i, v, targetType);
        }
        return wrapped;
    }

    @Override
    public T setToNull(int i) {
        return setValue(i, null);
    }

    @Override
    public T setToNull(String name) {
        for (int i : getAllIndexesOf(name)) {
            setToNull(i);
        }
        return wrapped;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AbstractData))
            return false;

        AbstractData<?> that = (AbstractData<?>)o;
        if (values.length != that.values.length)
            return false;

        if(this.protocolVersion != that.protocolVersion)
            return false;

        // Deserializing each value is slightly inefficient, but comparing
        // the bytes could in theory be wrong (for varint for instance, 2 values
        // can have different binary representation but be the same value due to
        // leading zeros). So we don't take any risk.
        for (int i = 0; i < values.length; i++) {
            DataType thisType = getType(i);
            DataType thatType = that.getType(i);
            if (!thisType.equals(thatType))
                return false;

            if ((values[i] == null) != (that.values[i] == null))
                return false;

            if (values[i] != null) {
                Object thisValue = this.codecFor(i).deserialize(this.values[i], this.protocolVersion);
                Object thatValue = that.codecFor(i).deserialize(that.values[i], that.protocolVersion);
                if (!thisValue.equals(thatValue))
                    return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // Same as equals
        int hash = 31;
        for (int i = 0; i < values.length; i++)
            hash += values[i] == null ? 1 : codecFor(i).deserialize(values[i], protocolVersion).hashCode();
        return hash;
    }
}
