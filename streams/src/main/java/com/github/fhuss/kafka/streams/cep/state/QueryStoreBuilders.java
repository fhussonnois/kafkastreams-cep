/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.Queried;
import com.github.fhuss.kafka.streams.cep.core.nfa.Stages;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.core.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.state.internal.QueriedInternal;
import com.github.fhuss.kafka.streams.cep.state.internal.builder.NFAStoreBuilder;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Objects;

/**
 * Helpers class to build required stores.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class QueryStoreBuilders<K, V> {

    private final String queryName;

    private final Stages<K, V> stages;

    private final StagesFactory<K, V> factory;

    /**
     * Creates a new {@link QueryStoreBuilders} instance.
     * @param queryName      the query name.
     * @param queryPattern   the query {@link Pattern} instance.
     */
    public QueryStoreBuilders(final String queryName, final Pattern<K, V> queryPattern) {
        Objects.requireNonNull(queryName, "queryName cannot be null");
        Objects.requireNonNull(queryPattern, "queryPattern cannot be null");
        this.queryName = queryName;
        this.factory = new StagesFactory<>();
        this.stages = factory.make(queryPattern);
    }


    /**
     * Build all {@link StoreBuilder} used to store aggregate states for stages.
     *
     * @return a new collection of {@link StoreBuilder}.
     */
    public StoreBuilder<AggregatesStateStore<K>> getAggregateStateStore(final Queried<K, V> queried) {
        final String storeName = QueryStores.getQueryAggregateStatesStoreName(queryName);

        final QueriedInternal<K, V> internal = new QueriedInternal<>(queried);

        KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier) internal.storeSupplier();
        if (supplier == null) {
            supplier = Stores.persistentKeyValueStore(storeName);
        }
        final StoreBuilder<AggregatesStateStore<K>> builder = QueryStores.aggregatesStoreBuilder(supplier);
        return mayEnableCaching(internal, mayEnableLogging(internal, builder));
    }

    /**
     * Build a persistent {@link StoreBuilder}.
     *
     * @param queried the {@link Queried} instance.
     *
     * @return a new {@link StoreBuilder} instance.
     */
    public StoreBuilder<NFAStateStore<K, V>> getNFAStateStore(final Queried<K, V> queried) {
        final String storeName = QueryStores.getQueryNFAStoreName(queryName);

        final QueriedInternal<K, V> internal = new QueriedInternal<>(queried);
        KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier) internal.storeSupplier();
        if (supplier == null) {
            supplier = Stores.persistentKeyValueStore(storeName);
        }
        final NFAStoreBuilder<K, V> builder = QueryStores.nfaStoreBuilder(
            supplier,
            stages.getAllStages(),
            internal.keySerde(),
            internal.valueSerde());
        return mayEnableCaching(internal, mayEnableLogging(internal, builder));
    }

    /**
     * Build a new {@link StoreBuilder} used to store match sequences.
     *
     * @param queried the {@link Queried} instance.
     *
     * @return a new {@link StoreBuilder} instance.
     */
    public StoreBuilder<SharedVersionedBufferStateStore<K, V>> getEventBufferStore(final Queried<K, V> queried) {
        final String storeName = QueryStores.getQueryEventBufferStoreName(queryName);

        final QueriedInternal<K, V> internal = new QueriedInternal<>(queried);
        KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier) internal.storeSupplier();
        if (supplier == null) {
            supplier = Stores.persistentKeyValueStore(storeName);
        }

        StoreBuilder<SharedVersionedBufferStateStore<K, V>> builder = QueryStores.bufferStoreBuilder(
            supplier,
            internal.keySerde(),
            internal.valueSerde()
        );
        return mayEnableCaching(internal, mayEnableLogging(internal, builder));
    }

    private <T extends StateStore> StoreBuilder<T> mayEnableCaching(final QueriedInternal<K, V> internal,
                                                                    final StoreBuilder<T> builder) {
        if (internal.cachingEnabled()) {
            builder.withCachingEnabled();
        } else {
            builder.withCachingDisabled();
        }
        return builder;
    }

    private <T extends StateStore> StoreBuilder<T> mayEnableLogging(final QueriedInternal<K, V> internal,
                                                                    final StoreBuilder<T> builder) {
        if (internal.loggingEnabled()) {
            builder.withLoggingEnabled(internal.logConfig());
        } else {
            builder.withLoggingDisabled();
        }
        return builder;
    }
}
