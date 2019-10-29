/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class Queried<K, V> {

    protected StoreSupplier<KeyValueStore<Bytes, byte[]>> storeSupplier;

    protected Serde<K> keySerde;
    protected Serde<V> valueSerde;
    protected boolean loggingEnabled = true;
    protected boolean cachingEnabled = true;
    protected Map<String, String> topicConfig = new HashMap<>();

    /**
     * Materialize a {@link KeyValueStore} using the provided {@link KeyValueBytesStoreSupplier}.
     *
     * @param supplier the {@link KeyValueBytesStoreSupplier} used to materialize the store
     * @param <K>      key type of the store
     * @param <V>      value type of the store
     * @return a new {@link Materialized} instance with the given supplier
     */
    public static <K, V> Queried<K, V> as(final KeyValueBytesStoreSupplier supplier) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        return new Queried<>(supplier);
    }

    /**
     * Create an instance of {@code Queried} with  a key {@link Serde} and a value {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default key serde as defined in config.
     *
     * @param keySerde      the record key serde
     * @param valueSerde    the record value serde
     * @param <K>   the record key type.
     * @param <V>   the record value type.
     * @return new {@code Queried} instance configured with the keySerde and valueSerde
     */
    public static <K, V> Queried<K, V> with(final Serde<K> keySerde, final Serde<V> valueSerde) {
        return new Queried<>(keySerde, valueSerde);
    }

    /**
     * Create an instance of {@code Queried} with  a key {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default key serde as defined in config.
     *
     * @param keySerde the key serde to use. If {@code null} the default key serde from config will be used
     * @param <K>   the record key type.
     * @param <V>   the record value type.
     * @return new {@code Queried} instance configured with the keySerde
     */
    public static <K, V> Queried<K, V> keySerde(final Serde<K> keySerde) {
        return with(keySerde, null);
    }

    /**
     * Create an instance of {@code Queried} with a value {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default value serde as defined in config.
     *
     * @param valueSerde the value serde to use. If {@code null} the default value serde from config will be used
     * @param <K>   the record key type.
     * @param <V>   the record value type.
     * @return new {@code Queried} instance configured with the valueSerde
     */
    public static <K, V> Queried<K, V> valueSerde(final Serde<V> valueSerde) {
        return with(null, valueSerde);
    }

    /**
     * Creates a new {@link Queried} instance.
     *
     * @param storeSupplier the {@link StoreSupplier} instance.
     */
    private Queried(final StoreSupplier<KeyValueStore<Bytes, byte[]>> storeSupplier) {
        this.storeSupplier = storeSupplier;
    }

    /**
     * Creates a new {@link Queried} instance.
     *
     * @param keySerde      the record key {@link Serde}.
     * @param valueSerde    the record value {@link Serde}.
     */
    private Queried(final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Creates a new {@link Queried} instance.
     */
    protected Queried() {
    }

    /**
     * Creates a new {@link Queried} instance.
     *
     * @param queried the {@link Queried} instance.
     */
    protected Queried(final Queried<K, V> queried) {
        this.keySerde = queried.keySerde;
        this.valueSerde = queried.valueSerde;
        this.cachingEnabled = queried.cachingEnabled;
        this.loggingEnabled = queried.loggingEnabled;
        this.topicConfig = queried.topicConfig;
        this.storeSupplier = queried.storeSupplier;
    }

    /**
     * Set the valueSerde the materialized {@link StateStore} will use.
     *
     * @param valueSerde the value {@link Serde} to use. If the {@link Serde} is null, then the default value
     *                   serde from configs will be used. If the serialized bytes is null for put operations,
     *                   it is treated as delete operation
     * @return {@code this}.
     */
    public Queried<K, V> withValueSerde(final Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

    /**
     * Set the keySerde the materialize {@link StateStore} will use.
     * @param keySerde  the key {@link Serde} to use. If the {@link Serde} is null, then the default key
     *                  serde from configs will be used
     * @return {@code this}.
     */
    public Queried<K, V> withKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
        return this;
    }

    /**
     * Indicates that a changelog should be created for the stores. The changelog will be created
     * with the provided configs.
     * <p>
     * Note: Any unrecognized configs will be ignored.
     * @param config    any configs that should be applied to the changelog
     * @return {@code this}.
     */
    public Queried<K, V> withLoggingEnabled(final Map<String, String> config) {
        loggingEnabled = true;
        this.topicConfig = config;
        return this;
    }

    /**
     * Disable change logging for the materialized {@link StateStore}s used for the query.
     * @return {@code this}.
     */
    public Queried<K, V> withLoggingDisabled() {
        loggingEnabled = false;
        this.topicConfig.clear();
        return this;
    }

    /**
     * Enable caching for the materialized {@link StateStore}s used for the query.
     * @return {@code this}.
     */
    public Queried<K, V> withCachingEnabled() {
        cachingEnabled = true;
        return this;
    }

    /**
     * Disable caching for the materialized {@link StateStore}s used for the query.
     * @return {@code this}.
     */
    public Queried<K, V> withCachingDisabled() {
        cachingEnabled = false;
        return this;
    }
}
