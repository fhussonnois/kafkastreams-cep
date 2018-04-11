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

import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.StagesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.List;

/**
 * Helpers class to build required stores.
 */
public class QueryStoreBuilders<K, V> {

    private final String queryName;

    private final List<Stage<K, V>> stages;

    private final StagesFactory<K, V> factory;

    /**
     * Creates a new {@link QueryStoreBuilders} instance.
     */
    public QueryStoreBuilders(final String queryName, final Pattern<K, V> pattern) {
        this.queryName = queryName;
        this.factory = new StagesFactory<>();
        this.stages = factory.make(pattern);
    }


    /**
     * Build all {@link StoreBuilder} used to store aggregate states for stages.
     *
     * @return a new collection of {@link StoreBuilder}.
     */
    public StoreBuilder<AggregatesStore<K>> getAggregateStateStores() {
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
    public StoreBuilder<NFAStore<K, V>> getNFAStateStoreBuilder(final Serde<K> keySerde,
                                                                final Serde<V> valueSerde) {
        final String storeName = QueryStores.getQueryNFAStoreName(queryName);
        final KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);

        return QueryStores.nfaStoreBuilder(storeSupplier, stages, keySerde, valueSerde);
    }

    /**
     * Build a new {@link StoreBuilder} used to store match sequences.
     *
     * @param keySerde      the key {@link Serde}.
     * @param valueSerde    the value {@link Serde}.
     * @return a new {@link StoreBuilder} instance.
     */
    public StoreBuilder<SharedVersionedBufferStore<K, V>> getEventBufferStoreBuilder(final Serde<K> keySerde,
                                                                                     final Serde<V> valueSerde) {
        final String storeName = QueryStores.getQueryEventBufferStoreName(queryName);
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);
        return QueryStores.bufferStoreBuilder(storeSupplier, keySerde, valueSerde);
    }
}
