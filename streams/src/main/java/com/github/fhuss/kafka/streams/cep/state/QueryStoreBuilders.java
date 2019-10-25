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
package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.core.nfa.Stages;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.core.pattern.StagesFactory;
import org.apache.kafka.common.serialization.Serde;
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
     * @param queryName the complex query name.
     * @param pattern   the complex pattern.
     */
    public QueryStoreBuilders(final String queryName, final Pattern<K, V> pattern) {
        Objects.requireNonNull(queryName, "queryName cannot be null");
        Objects.requireNonNull(pattern, "pattern cannot be null");
        this.queryName = queryName;
        this.factory = new StagesFactory<>();
        this.stages = factory.make(pattern);
    }


    /**
     * Build all {@link StoreBuilder} used to store aggregate states for stages.
     *
     * @return a new collection of {@link StoreBuilder}.
     */
    public StoreBuilder<AggregatesStateStore<K>> getAggregateStateStores() {
        final String storeName = QueryStores.getQueryAggregateStatesStoreName(queryName);
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);

        return QueryStores.aggregatesStoreBuilder(storeSupplier);
    }

    /**
     * Build a persistent {@link StoreBuilder}.
     *
     * @param keySerde      the key {@link Serde}.
     * @param valueSerde    the value {@link Serde}.
     * @return a new {@link StoreBuilder} instance.
     */
    public StoreBuilder<NFAStateStore<K, V>> getNFAStateStoreBuilder(final Serde<K> keySerde,
                                                                final Serde<V> valueSerde) {
        final String storeName = QueryStores.getQueryNFAStoreName(queryName);
        final KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);

        return QueryStores.nfaStoreBuilder(storeSupplier, stages.getAllStages(), keySerde, valueSerde);
    }

    /**
     * Build a new {@link StoreBuilder} used to store match sequences.
     *
     * @param keySerde      the key {@link Serde}.
     * @param valueSerde    the value {@link Serde}.
     * @return a new {@link StoreBuilder} instance.
     */
    public StoreBuilder<SharedVersionedBufferStateStore<K, V>> getEventBufferStoreBuilder(final Serde<K> keySerde,
                                                                                     final Serde<V> valueSerde) {
        final String storeName = QueryStores.getQueryEventBufferStoreName(queryName);
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);
        return QueryStores.bufferStoreBuilder(storeSupplier, keySerde, valueSerde);
    }
}
