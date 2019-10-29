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
package com.github.fhuss.kafka.streams.cep.core.state;

import com.github.fhuss.kafka.streams.cep.core.state.internal.Aggregate;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Aggregated;

/**
 * Simple class to wrap a {@link AggregatesStore}.
 *
 * @param <K>   the record key type.
 */
public class States<K> {

    private final AggregatesStore<K> store;

    private final long sequence;

    private final K key;

    /**
     * Creates a new {@link States} instance.
     *
     * @param states    the aggregates store.
     * @param key       the record key used to store aggregates.
     * @param sequence  The sequence number (aka run) used to store aggregates.
     */
    public States(final AggregatesStore<K> states, K key, long sequence) {
        this.store = states;
        this.key = key;
        this.sequence = sequence;
    }

    /**
     * Retrieve the value state for the specified key.
     *
     * @param state the state name.
     * @param <T> the of default value.
     * @return <code>null</code> if no state exists for the given key.
     */
    public <T> T get(final String state) {
        T v = getOrNull(state);
        if (v == null) throw new UnknownAggregateException(state);
        return v;
    }

    /**
     * Retrieve the value state for the specified key.
     *
     * @param key the object key.
     * @param def the default value.
     * @param <T> the of default value.
     * @return {@literal def} if no state exists for the given key.
     */
    public <T> T getOrElse(String key, T def) {
        T val = getOrNull(key);
        return val != null ? val : def;
    }

    private <T> T getOrNull(String state) {
        Aggregated<K> aggregated = new Aggregated<>(key, new Aggregate(state, sequence));
        return store.find(aggregated);
    }

    static class UnknownAggregateException extends RuntimeException {
        /**
         * Creates a new {@link UnknownAggregateException} instance.
         *
         * @param state the state name.
         */
        UnknownAggregateException(final String state) {
            super("No state found for name '" + state + "'");
        }
    }
}
