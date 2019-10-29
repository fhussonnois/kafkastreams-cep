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
package com.github.fhuss.kafka.streams.cep.state.internal.builder;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractStoreBuilder<K, V, T extends StateStore> implements StoreBuilder<T> {

    private final String name;
    private Map<String, String> logConfig = new HashMap<>();
    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    boolean enableCaching;
    boolean enableLogging = true;

    AbstractStoreBuilder(final String name,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde) {
        Objects.requireNonNull(name, "name can't be null");
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StoreBuilder<T> withCachingEnabled() {
        enableCaching = true;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StoreBuilder<T> withLoggingEnabled(final Map<String, String> config) {
        Objects.requireNonNull(config, "config can't be null");
        enableLogging = true;
        logConfig = config;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StoreBuilder<T> withLoggingDisabled() {
        enableLogging = false;
        logConfig.clear();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StoreBuilder<T> withCachingDisabled() {
        enableCaching = false;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean loggingEnabled() {
        return enableLogging;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return name;
    }
}