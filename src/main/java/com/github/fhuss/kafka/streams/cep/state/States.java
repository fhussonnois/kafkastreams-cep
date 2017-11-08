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

import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Simple class to wrap a {@link KeyValueStore}.
 */
public class States {

    private final StateStoreProvider provider;

    private long sequence;

    /**
     * Creates a new {@link States} instance.
     * @param provider
     * @param sequence
     */
    public States(StateStoreProvider provider, long sequence) {
        this.provider = provider;
        this.sequence = sequence;
    }

    /**
     * Retrieve the value state for the specified key.
     *
     * @param key the object key.
     * @return <code>null</code> if no state exists for the given key.
     */
    public Object get(String key) {
        ValueStore store = provider.getValueStore(key, sequence);
        return ( store != null ) ? store.get() : null;
    }

    /**
     * Retrieve the value state for the specified key.
     *
     * @param key the object key.
     * @param def the default value.
     * @param <T> the of default value.
     * @return {@literal def} if no state exists for the given key.
     */
    @SuppressWarnings("unchecked")
    public <T> T getOrElse(String key, T def) {
        ValueStore store = provider.getValueStore(key, sequence);
        if ( store != null ) {
            T val = (T) store.get();
            return val != null ? val : def;
        }
        else return def;
    }
}
