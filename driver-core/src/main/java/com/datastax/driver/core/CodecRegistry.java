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

import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.TypeCodec.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;

/**
 * A registry for {@link TypeCodec}s. When the driver
 * needs to serialize or deserialize an object,
 * it will lookup in the {@link CodecRegistry} for
 * a suitable codec.
 *
 * <h3>Usage</h3>
 *
 * <p>
 * To build a {@link CodecRegistry} instance with all the default codecs used
 * by the Java driver, simply use the following:
 *
 * <pre>
 * CodecRegistry myCodecRegistry = new CodecRegistry().registerDefaults();
 * </pre>
 *
 * Custom {@link TypeCodec}s can be added to a {@link CodecRegistry} via the {@code register} methods:
 *
 * <pre>
 * CodecRegistry myCodecRegistry = new CodecRegistry()
 *    .register(myCodec1, myCodec2, myCodec3)
 *    .registerDefaults(); // to ensure that all CQL types are covered by a matching codec
 * </pre>
 *
 * To be used by the driver, {@link CodecRegistry} instances must then
 * be associated with a {@link Cluster} instance:
 *
 * <pre>
 * CodecRegistry myCodecRegistry = ...
 * Cluster cluster = new Cluster.builder().withCodecRegistry(myCodecRegistry).build();
 * </pre>
 *
 * To retrieve the {@link CodecRegistry} instance associated with a Cluster, do the
 * following:
 *
 * <pre>
 * CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
 * </pre>
 *
 * Registered codecs will remain in a reservoir where they can
 * potentially be selected to perform serialization or
 * deserialization operations requested by the application.
 * <p>
 * Note that the order in which codecs are added to the registry matters;
 * any codec that is registered may potentially override an already registered one.
 * When looking for a matching codec, the registry will consider all known codecs
 * starting with the last ones and will return the first matching candidate,
 * even if another codec would be a better match.
 * <p>
 * For most users, the default {@link CodecRegistry} instance should be adequate.
 * <strong>Using customized codecs is an advanced feature and registries
 * that do not have enough codecs or codecs that do not comply with the native protocol
 * specifications may cause the driver to crash.</strong>
 *
 * <h3>Examples</h3>
 *
 * <p>
 * <strong>1. Globally use a custom codec to handle a given CQL type</strong>
 *
 * <p>
 * E.g. let's suppose you want to have all your CQL timestamps
 * deserialized as {@code java.time.DateTime} instances:
 *
 * <pre>
 * TypeCodec&lt;DateTime> timestampCodec = ...
 * new CodecRegistry().register(timestampCodec).registerDefaults();
 * </pre>
 *
 * <p>
 * <strong>2. Use a special codec for a specific keyspace, table and column</strong>
 *
 * <p>
 * E.g. let's suppose you want to have a certain varchar column deserialized as a JSON object:
 *
 * <pre>
 * TypeCodec&lt;MyJsonObject> jsonCodec = ...
 * new CodecRegistry()
 *      .register(jsonCodec, "myKeyspace.myTable.myColumn") // will be used only for this specific column
 *      .registerDefaults();
 * </pre>
 *
 * <h3>Notes</h3>
 *
 * <p>
 * When a {@link CodecRegistry} cannot find a suitable codec among all registered codecs,
 * it will attempt to create a suitable codec.
 * <p>
 * If the creation succeeds, that codec is added to the list of known codecs and is returned;
 * otherwise, a {@link CodecNotFoundException} is thrown.
 * <p>
 * Note that {@link CodecRegistry} instances can only create codecs in very limited situations:
 * <ol>
 *     <li>Codecs for Enums are created on the fly using {@link EnumCodec};</li>
 *     <li>Codecs for {@link UserType user types} are created on the fly using  {@link UDTCodec};</li>
 *     <li>Codecs for {@link TupleType tuple types} are created on the fly using  {@link TupleCodec};</li>
 *     <li>Codecs for collections of Enums, user types and tuple types are created on the fly using
 *     {@link ListCodec}, {@link SetCodec} and {@link MapCodec}.</li>
 * </ol>
 * Other combinations of Java and CQL types cannot have their codecs created on-the-fly;
 * such codecs must be manually registered.
 * <p>
 * Note that the default set of codecs has no support for
 * <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java">Cassandra custom types</a>;
 * to be able to deserialize values of such types, you need to manually register an appropriate codec.
 * <p>
 * {@link CodecRegistry} instances are thread-safe.
 *
 */
@SuppressWarnings("unchecked")
public final class CodecRegistry {

    private static final Logger logger = LoggerFactory.getLogger(CodecRegistry.class);

    private static final ImmutableSet<TypeCodec<?>> PRIMITIVE_CODECS = ImmutableSet.of(
        BlobCodec.instance,
        BooleanCodec.instance,
        SmallIntCodec.instance,
        TinyIntCodec.instance,
        IntCodec.instance,
        BigintCodec.instance,
        CounterCodec.instance,
        DoubleCodec.instance,
        FloatCodec.instance,
        VarintCodec.instance,
        DecimalCodec.instance,
        VarcharCodec.instance,
        AsciiCodec.instance,
        TimestampCodec.instance,
        DateCodec.instance,
        TimeCodec.instance,
        UUIDCodec.instance,
        TimeUUIDCodec.instance,
        InetCodec.instance
    );

    /**
     * The default set of codecs used by the Java driver.
     * They contain codecs to handle native types, and collections thereof (lists, sets and maps).
     *
     * Note that the default set of codecs has no support for
     * <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java">Cassandra custom types</a>;
     * to be able to deserialize values of such types, you need to manually register an appropriate codec.
     */
    private static final ImmutableList<TypeCodec<?>> DEFAULT_CODECS;

    static {
        ImmutableList.Builder<TypeCodec<?>> builder = new ImmutableList.Builder<TypeCodec<?>>();
        builder.addAll(PRIMITIVE_CODECS);
        for (TypeCodec<?> primitiveCodec1 : PRIMITIVE_CODECS) {
            builder.add(new ListCodec(primitiveCodec1));
            builder.add(new SetCodec(primitiveCodec1));
            for (TypeCodec<?> primitiveCodec2 : PRIMITIVE_CODECS) {
                builder.add(new MapCodec(primitiveCodec1, primitiveCodec2));
            }
        }
        DEFAULT_CODECS = builder.build();
    }

    private static final class CacheKey {

        private final TypeToken<?> javaType;

        private final DataType cqlType;

        public CacheKey(TypeToken<?> javaType, DataType cqlType) {
            this.javaType = javaType;
            this.cqlType = cqlType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CacheKey cacheKey = (CacheKey)o;
            return Objects.equal(javaType, cacheKey.javaType) && Objects.equal(cqlType, cacheKey.cqlType);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(javaType, cqlType);
        }
    }

    private final CopyOnWriteArrayList<TypeCodec<?>> codecs;

    private final ConcurrentMap<String, TypeCodec<?>> overrides;

    private final LoadingCache<CacheKey, TypeCodec<?>> cache;

    public CodecRegistry() {
        this.codecs = new CopyOnWriteArrayList<TypeCodec<?>>(DEFAULT_CODECS);
        this.overrides = new ConcurrentHashMap<String, TypeCodec<?>>();
        this.cache = CacheBuilder.newBuilder()
            .build(
                new CacheLoader<CacheKey, TypeCodec<?>>() {
                    public TypeCodec<?> load(CacheKey cacheKey) {
                        return findCodec(cacheKey.cqlType, cacheKey.javaType);
                    }
                });
    }

    /**
     * Register the given codec with this registry.
     *
     * @param codec The codec to add to the registry.
     * @return this CodecRegistry (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?> codec) {
        return register(codec, false);
    }

    /**
     * Register the given codec with this registry.
     *
     * @param codec The codec to add to the registry.
     * @param includeDerivedCollectionCodecs If {@code true}, register not only the given codec
     * but also collection codecs whose element types are handled by the given codec.
     * @return this CodecRegistry (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?> codec, boolean includeDerivedCollectionCodecs) {
        return register(Collections.singleton(codec), includeDerivedCollectionCodecs);
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?>... codecs) {
        return register(Arrays.asList(codecs), false);
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(Iterable<? extends TypeCodec<?>> codecs) {
        return register(codecs, false);
    }

    /**
     * Add the given codecs to the list of known codecs of this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @param includeDerivedCollectionCodecs If {@code true}, register not only the given codecs
     * but also collection codecs whose element types are handled by the given codecs.
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(Iterable<? extends TypeCodec<?>> codecs, boolean includeDerivedCollectionCodecs) {
        for (TypeCodec<?> codec : codecs) {
            if(includeDerivedCollectionCodecs){
                // a map keys and values of the same given type
                this.codecs.add(0, new MapCodec(codec, codec));
                // map codecs for combinations of the given type with all primitive types
                for (TypeCodec<?> primitiveCodec : PRIMITIVE_CODECS) {
                    this.codecs.add(0, new MapCodec(primitiveCodec, codec));
                    this.codecs.add(0, new MapCodec(codec, primitiveCodec));
                }
                this.codecs.add(0, new ListCodec(codec));
                this.codecs.add(0, new SetCodec(codec));
            }
            this.codecs.add(0, codec);
        }
        return this;
    }

    /**
     * Register the given codec with this registry as an "overriding" codec.
     * <p>
     * This codec will only be used if the value being serialized or deserialized
     * belongs to the specified qualified name.
     * <p>
     * Qualified names shoud be specified in the form of {@code keyspace.table.column} or {@code keyspace.usertype.field}.
     * <p>
     * <strong>IMPORTANT</strong>: Overriding codecs can only be used in conjunction with {@link GettableData} instances,
     * such as{@link BoundStatement}s and {@link Row}s.
     * It <em>cannot</em> be used with other instances of {@link Statement}, such
     * as {@link SimpleStatement} or with the {@link com.datastax.driver.core.querybuilder.QueryBuilder query builder}.
     * <p>
     * <em>For this reason, if you plan to use overriding codecs, be sure to only use {@link PreparedStatement}s.</em>
     *
     * @param codec The overriding codec.
     * @param fqdn The fully-qualified name of the table column or UDT field to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?> codec, String fqdn) {
        checkNotNull(fqdn, "Parameter fqdn cannot be null");
        overrides.put(fqdn, codec);
        return this;
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given value.
     * <p>
     * This method takes an actual Java object and tries to locate a suitable codec for it.
     * For this reason, codecs must perform a {@link TypeCodec#accepts(Object) "manual" inspection}
     * of the object to determine if they can accept it or not, which, depending on the implementations,
     * can be an expensive operation; besides, the resulting codec cannot be cached.
     * Therefore there might be a performance penalty when using this method.
     * <p>
     * Furthermore, this method would return the first matching codec, regardless of its accepted {@link DataType CQL type}.
     * For this reason, this method might not return the most accurate codec and should be
     * reserved for situations where the runtime object to be serialized is not available or unknown.
     * In the Java driver, this happens mainly when serializing a value in a {@link SimpleStatement}
     * or in the {@link com.datastax.driver.core.querybuilder.QueryBuilder}, where no CQL type information
     * is available.
     * <p>
     * Regular users should avoid using this method.
     * <p>
     * Codecs returned by this method are NOT cached.
     *
     * @param value The value the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(T value) {
        checkNotNull(value, "Parameter value cannot be null");
        logger.debug("Looking for codec [ANY <-> {}]", value.getClass());
        TypeCodec<T> codec = findCodec(null, value);
        logger.debug("Codec found: {}", codec);
        return codec;
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}.
     * <p>
     * This method would return the first matching codec, regardless of its accepted Java type.
     * For this reason, this method might not return the most accurate codec and should be
     * reserved for situations where the runtime type is not available or unknown.
     * In the Java driver, this happens mainly when deserializing a value using the
     * {@link AbstractGettableData#getObject(int)} method.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType) throws CodecNotFoundException {
        return lookupCache(cqlType, null);
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given Java class.
     * <p>
     * This method can only handle raw (non-parameterized) Java types.
     * For parameterized types, use {@link #codecFor(DataType, TypeToken)} instead.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec accepts a Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even if another codec is a better match</em>.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The Java type the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType) throws CodecNotFoundException {
        return codecFor(cqlType, TypeToken.of(javaType));
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given Java type.
     * <p>
     * This method handles parameterized types thanks to Guava's {@link TypeToken} API, and should
     * therefore be preferred over {@link #codecFor(DataType, Class)}.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec accepts a Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even if another codec is a better match</em>.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The {@link TypeToken Java type} the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return lookupCache(cqlType, javaType);
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given value.
     * <p>
     * This method takes an actual Java object and tries to locate a suitable codec for it.
     * For this reason, codecs must perform a {@link TypeCodec#accepts(Object) "manual" inspection}
     * of the object to determine if they can accept it or not, which, depending on the implementations,
     * can be an expensive operation; besides, the resulting codec cannot be cached.
     * Therefore there might be a performance penalty when using this method.
     * <p>
     * Codecs returned by this method are NOT cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param value The value the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, T value) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(value, "Parameter value cannot be null");
        return findCodec(cqlType, value);
    }

    /**
     * Return an overriding {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and applies only to the given keyspace, table and column.
     * <p>
     * If such an overriding codec cannot be found, the registry attempts to find
     * a globally-registered one and returns it instead.
     * <p>
     * This method would return the first matching codec, regardless of its accepted Java type.
     * For this reason, this method might not return the most accurate codec and should be
     * reserved for situations where the runtime object to be serialized is not available or unknown.
     * In the Java driver, this happens mainly when deserializing a value using the
     * {@link AbstractGettableData#getObject(int)} method.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param fqdn The fully-qualified name of the table column or UDT field to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return An overriding codec for the given keyspace, table and column, if any; otherwise, a globally-registered suitable codec.
     * @throws CodecNotFoundException if an overriding codec is found but does not accept the given {@link DataType CQL type};
     * or if no suitable codec could be found at all.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, String fqdn) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, null, fqdn);
        if (codec != null)
            return codec;
        return codecFor(cqlType);
    }

    /**
     * Return an overriding {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given Java class, and that applies only to the given keyspace, table and column.
     * <p>
     * If such an overriding codec cannot be found, the registry attempts to find
     * a globally-registered one and returns it instead.
     * <p>
     * This method can only handle raw (non-parameterized) Java types.
     * For parameterized types, use {@link #codecFor(DataType, TypeToken, String)} instead.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec accepts a Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even if another codec is a better match</em>.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The Java type the codec should accept; must not be {@code null}.
     * @param fqdn The fully-qualified name of the table column or UDT field to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return An overriding codec for the given keyspace, table and column, if any; otherwise, a globally-registered suitable codec.
     * @throws CodecNotFoundException if an overriding codec is found but does not accept the given {@link DataType CQL type}
     * and the given Java class; or if no suitable codec could be found at all.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType, String fqdn) throws CodecNotFoundException {
        return codecFor(cqlType, TypeToken.of(javaType), fqdn);
    }

    /**
     * Return an overriding {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given Java type, and that applies only to the given keyspace, table and column.
     * <p>
     * If such an overriding codec cannot be found, the registry attempts to find
     * a globally-registered one and returns it instead.
     * <p>
     * This method handles parameterized types thanks to Guava's {@link TypeToken} API, and should
     * therefore be preferred over {@link #codecFor(DataType, Class, String)}.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec accepts a Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even if another codec is a better match</em>.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The {@link TypeToken Java type} the codec should accept; must not be {@code null}.
     * @param fqdn The fully-qualified name of the table column or UDT field to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return An overriding codec for the given keyspace, table and column, if any; otherwise, a globally-registered suitable codec.
     * @throws CodecNotFoundException if an overriding codec is found but does not accept the given {@link DataType CQL type}
     * and the given Java type; or if no suitable codec could be found at all.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType, String fqdn) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(javaType, "Parameter javaType cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, javaType, fqdn);
        if (codec != null)
            return codec;
        return codecFor(cqlType, javaType);
    }

    /**
     * Return an overriding {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given value, and that applies only to the given keyspace, table and column.
     * <p>
     * If such an overriding codec cannot be found, the registry attempts to find
     * a globally-registered one and returns it instead.
     * <p>
     * This method takes an actual Java object and tries to locate a suitable codec for it.
     * For this reason, codecs must perform a {@link TypeCodec#accepts(Object) "manual" inspection}
     * of the object to determine if they can accept it or not, which, depending on the implementations,
     * can be an expensive operation; besides, the resulting codec cannot be cached.
     * Therefore there might be a performance penalty when using this method.
     * <p>
     * Codecs returned by this method are NOT cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param value The value the codec should accept; must not be {@code null}.
     * @param fqdn The fully-qualified name of the table column or UDT field to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return An overriding codec for the given keyspace, table and column, if any, otherwise, a globally-registered suitable codec.
     * @throws CodecNotFoundException if an overriding codec is found but does not accept the given value;
     * or if no suitable codec could be found at all.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, T value, String fqdn) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(value, "Parameter value cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, value, fqdn);
        if (codec != null)
            return codec;
        return codecFor(cqlType, value);
    }

    private <T> TypeCodec<T> lookupCache(DataType cqlType, TypeToken<T> javaType) {
        logger.debug("Looking up codec for {} <-> {}", cqlType, javaType);
        CacheKey cacheKey = new CacheKey(javaType, cqlType);
        try {
            return (TypeCodec<T>)cache.get(cacheKey);
        } catch (UncheckedExecutionException e) {
            if(e.getCause() instanceof CodecNotFoundException)
                throw (CodecNotFoundException)e.getCause();
            throw new CodecNotFoundException(e.getCause(), cqlType, javaType);
        } catch (ExecutionException e) {
            throw new CodecNotFoundException(e.getCause(), cqlType, javaType);
        }
    }

    private <T> TypeCodec<T> findCodec(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        logger.debug("Looking for codec [{} <-> {}]", cqlType, javaType == null ? "ANY" : javaType);
        for (TypeCodec<?> codec : codecs) {
            if (codec.accepts(cqlType) && (javaType == null || codec.accepts(javaType))) {
                logger.debug("Codec found: {}", codec);
                return (TypeCodec<T>)codec;
            }
        }
        // codec not found among the reservoir of known codecs, try to create an ad-hoc codec;
        // this situation should happen mainly when dealing with enums, tuples and UDTs,
        // and collections thereof.
        TypeCodec<T> codec = maybeCreateCodec(cqlType, javaType);
        if(codec == null)
            throw logErrorAndThrow(cqlType, javaType);
        if (!codec.accepts(cqlType) || (javaType != null && !codec.accepts(javaType)))
            throw logErrorAndThrow(cqlType, javaType);
        logger.debug("Codec found: {}", codec);
        codecs.addIfAbsent(codec);
        return codec;
    }

    private <T> TypeCodec<T> findCodec(DataType cqlType, T value) {
        checkNotNull(value, "Parameter value cannot be null");
        logger.debug("Looking for codec [{} <-> {}]", cqlType == null ? "ANY" : cqlType, value.getClass());
        for (TypeCodec<?> codec : codecs) {
            if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value)) {
                logger.debug("Codec found: {}", codec);
                return (TypeCodec<T>)codec;
            }
        }
        // codec not found among the reservoir of known codecs, try to create an ad-hoc codec;
        // this situation should happen mainly when dealing with enums, tuples and UDTs,
        // and collections thereof.
        TypeCodec<T> codec = maybeCreateCodec(cqlType, value);
        if(codec == null)
            throw logErrorAndThrow(cqlType, TypeToken.of(value.getClass()));
        if((cqlType != null && !codec.accepts(cqlType)) || !codec.accepts(value))
            throw logErrorAndThrow(cqlType, TypeToken.of(value.getClass()));
        logger.debug("Codec found: {}", codec);
        codecs.addIfAbsent(codec);
        return codec;
    }

    private <T> TypeCodec<T> findOverridingCodec(DataType cqlType, TypeToken<T> javaType, String fqdn) {
        if (fqdn != null) {
            logger.debug("Looking for overriding codec for FQDN {}", fqdn);
            if (overrides.containsKey(fqdn)) {
                TypeCodec<?> codec = overrides.get(fqdn);
                if ((cqlType == null || codec.accepts(cqlType)) && (javaType == null || codec.accepts(javaType))) {
                    logger.debug("Overriding codec found for FQDN {}: {}", fqdn, codec);
                    return (TypeCodec<T>)codec;
                } else {
                    throw logErrorAndThrow(codec, cqlType, javaType, fqdn);
                }
            }
        }
        return null;
    }

    private <T> TypeCodec<T> findOverridingCodec(DataType cqlType, T value, String fqdn) {
        if (fqdn != null) {
            logger.debug("Looking for overriding codec for FQDN {}", fqdn);
            if (overrides.containsKey(fqdn)) {
                TypeCodec<?> codec = overrides.get(fqdn);
                if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value)) { // value cannot be null
                    logger.debug("Overriding codec found for FQDN {}: {}", fqdn, codec);
                    return (TypeCodec<T>)codec;
                } else {
                    throw logErrorAndThrow(codec, cqlType, TypeToken.of((Class<T>)value.getClass()), fqdn);
                }
            }
        }
        return null;
    }

    private <T> TypeCodec<T> maybeCreateCodec(DataType cqlType, TypeToken<T> javaType) {
        if(javaType != null && Enum.class.isAssignableFrom(javaType.getRawType())) {
            return new EnumCodec(javaType.getRawType());
        } else if (cqlType.getName() == DataType.Name.LIST && (javaType == null || List.class.isAssignableFrom(javaType.getRawType()))) {
            TypeToken<?> elementType = null;
            if(javaType != null && javaType.getType() instanceof ParameterizedType){
                elementType = TypeToken.of(((ParameterizedType)javaType.getType()).getActualTypeArguments()[0]);
            }
            TypeCodec<?> eltCodec = findCodec(cqlType.getTypeArguments().get(0), elementType);
            return new ListCodec(eltCodec);
        } else if (cqlType.getName() == DataType.Name.SET && (javaType == null || Set.class.isAssignableFrom(javaType.getRawType()))) {
            TypeToken<?> elementType = null;
            if(javaType != null && javaType.getType() instanceof ParameterizedType){
                elementType = TypeToken.of(((ParameterizedType)javaType.getType()).getActualTypeArguments()[0]);
            }
            TypeCodec<?> eltCodec = findCodec(cqlType.getTypeArguments().get(0), elementType);
            return new SetCodec(eltCodec);
        } else if (cqlType.getName() == DataType.Name.MAP && (javaType == null || Map.class.isAssignableFrom(javaType.getRawType()))) {
            TypeToken<?> keyType = null;
            TypeToken<?> valueType = null;
            if(javaType != null && javaType.getType() instanceof ParameterizedType){
                keyType = TypeToken.of(((ParameterizedType)javaType.getType()).getActualTypeArguments()[0]);
                valueType = TypeToken.of(((ParameterizedType)javaType.getType()).getActualTypeArguments()[1]);
            }
            return new MapCodec(findCodec(cqlType.getTypeArguments().get(0), keyType), findCodec(cqlType.getTypeArguments().get(1), valueType));
        } else if(cqlType instanceof TupleType && (javaType == null || TupleValue.class.isAssignableFrom(javaType.getRawType()))) {
            return (TypeCodec<T>)new TupleCodec((TupleType)cqlType);
        } else if(cqlType instanceof UserType && (javaType == null || UDTValue.class.isAssignableFrom(javaType.getRawType()))) {
            return (TypeCodec<T>)new UDTCodec((UserType)cqlType);
        }
        return null;
    }

    private <T> TypeCodec<T> maybeCreateCodec(DataType cqlType, T value) {
        if(value instanceof Enum) {
            return new EnumCodec(value.getClass());
        } else if ((cqlType == null || cqlType.getName() == DataType.Name.LIST) && value instanceof List) {
            if(((List)value).isEmpty()) {
                return new ListCodec(BlobCodec.instance);
            } else {
                DataType elementType = cqlType == null || cqlType.getTypeArguments().isEmpty() ? null : cqlType.getTypeArguments().get(0);
                return new ListCodec(findCodec(elementType, ((List)value).iterator().next()));
            }
        } else if ((cqlType == null || cqlType.getName() == DataType.Name.SET) && value instanceof Set) {
            if(((Set)value).isEmpty()) {
                return new SetCodec(BlobCodec.instance);
            } else {
                DataType elementType = cqlType == null || cqlType.getTypeArguments().isEmpty() ? null : cqlType.getTypeArguments().get(0);
                return new SetCodec(findCodec(elementType, ((Set)value).iterator().next()));
            }
        } else if ((cqlType == null || cqlType.getName() == DataType.Name.MAP) && value instanceof Map) {
            if(((Map)value).isEmpty()) {
                return new MapCodec(BlobCodec.instance, BlobCodec.instance);
            } else {
                DataType keyType = cqlType == null || cqlType.getTypeArguments().size() < 1 ? null : cqlType.getTypeArguments().get(0);
                DataType valueType = cqlType == null || cqlType.getTypeArguments().size() < 2 ? null : cqlType.getTypeArguments().get(1);
                Map.Entry entry = (Map.Entry)((Map)value).entrySet().iterator().next();
                return new MapCodec(findCodec(keyType, entry.getKey()), findCodec(valueType, entry.getValue()));
            }
        } else if((cqlType == null || cqlType.getName() == DataType.Name.TUPLE) && value instanceof TupleValue) {
            return (TypeCodec<T>)new TupleCodec(((TupleValue)value).getType());
        } else if((cqlType == null || cqlType.getName() == DataType.Name.UDT) && value instanceof UDTValue) {
            return (TypeCodec<T>)new UDTCodec(((UDTValue)value).getType());
        }
        return null;
    }

    private CodecNotFoundException logErrorAndThrow(DataType cqlType, TypeToken<?> javaType) {
        String msg = String.format("Codec not found for required pair: [%s <-> %s]",
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        logger.error(msg);
        return new CodecNotFoundException(msg, cqlType, javaType);
    }

    private CodecNotFoundException logErrorAndThrow(TypeCodec<?> codec, DataType cqlType, TypeToken<?> javaType, String fqdn) {
        String msg = String.format("Found overriding codec %s for %s but it does not accept required pair: [%s <-> %s]",
            codec,
            fqdn,
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        logger.error(msg);
        return new CodecNotFoundException(msg, cqlType, javaType);
    }

}
