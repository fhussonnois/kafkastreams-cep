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

import com.github.fhuss.kafka.streams.cep.core.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.core.state.AggregatesStore;
import com.github.fhuss.kafka.streams.cep.state.internal.builder.AggregatesStoreBuilder;
import com.github.fhuss.kafka.streams.cep.state.internal.builder.BufferStoreBuilder;
import com.github.fhuss.kafka.streams.cep.state.internal.builder.NFAStoreBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.List;

public class QueryStores {

    private static final String SUFFIX_BUFFER_EVENT_STORE = "-streamscep-matched";

    private static final String SUFFIX_NFA_STORE          = "-streamscep-states";

    private static final String SUFFIX_STATES_STORE       = "-streamscep-aggregates";

    public static String getQueryAggregateStatesStoreName(final String queryName) {
        return getQueryStateName(SUFFIX_STATES_STORE, queryName);
    }

    public static String getQueryNFAStoreName(final String queryName) {
        return getQueryStateName(SUFFIX_NFA_STORE, queryName);
    }

    public static String getQueryEventBufferStoreName(final String queryName) {
        return getQueryStateName(SUFFIX_BUFFER_EVENT_STORE, queryName);
    }

    private static String getQueryStateName(final String suffix, final String queryName) {
        return (queryName + suffix).toLowerCase();
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link AggregatesStore}.
     *
     * @param supplier      a {@link KeyValueBytesStoreSupplier}
     * @param <K>           key type
     * @return an instance of {@link StoreBuilder} than can build a {@link AggregatesStore}
     **/
    public static <K> StoreBuilder<AggregatesStateStore<K>> aggregatesStoreBuilder(final KeyValueBytesStoreSupplier supplier) {
        return new AggregatesStoreBuilder<>(supplier, Time.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link AggregatesStore}.
     *
     * @param supplier      the instance of {@link KeyValueBytesStoreSupplier} to use
     * @param keySerde      the key serde
     * @param valSerde      the value serde
     * @param <K>           the type of keys
     * @param <V>           the type of values
     * @return an instance of {@link StoreBuilder} than can build a {@link AggregatesStore}
     **/
    public static <K, V> StoreBuilder<SharedVersionedBufferStateStore<K, V>> bufferStoreBuilder(
            final KeyValueBytesStoreSupplier supplier,
            final Serde<K> keySerde,
            final Serde<V> valSerde) {
        return new BufferStoreBuilder<>(supplier, keySerde, valSerde, Time.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link AggregatesStore}.
     *
     * @param supplier      the instance of {@link KeyValueBytesStoreSupplier} to use
     * @param stages        the list of {@link Stage} instance.
     * @param keySerde      the key serde
     * @param valSerde      the value serde
     * @param <K>           the type of keys
     * @param <V>           the type of values
     * @return an instance of {@link StoreBuilder} than can build a {@link AggregatesStore}
     * */
    public static <K, V> NFAStoreBuilder<K, V> nfaStoreBuilder(
            final KeyValueBytesStoreSupplier supplier,
            final List<Stage<K, V>> stages,
            final Serde<K> keySerde,
            final Serde<V> valSerde) {
        return new NFAStoreBuilder<>(supplier, stages, keySerde, valSerde, Time.SYSTEM);
    }
}
