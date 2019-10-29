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

import com.github.fhuss.kafka.streams.cep.core.state.AggregatesStore;
import com.github.fhuss.kafka.streams.cep.state.AggregatesStateStore;
import com.github.fhuss.kafka.streams.cep.state.internal.AggregatesStoreImpl;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Default class to build {@link AggregatesStore} instance.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class AggregatesStoreBuilder<K, V> extends AbstractStoreBuilder<K, V, AggregatesStateStore<K>> {

    private final KeyValueBytesStoreSupplier storeSupplier;

    public AggregatesStoreBuilder(final KeyValueBytesStoreSupplier storeSupplier) {
        super(storeSupplier.name(), null, null);
        this.storeSupplier = storeSupplier;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AggregatesStateStore<K> build() {
        final StoreBuilder<KeyValueStore<Bytes, byte[]>> builder = Stores.keyValueStoreBuilder(
            storeSupplier,
            Serdes.Bytes(),
            Serdes.ByteArray());

        if (enableLogging) {
            builder.withLoggingEnabled(logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (enableCaching) {
            builder.withCachingEnabled();
        }
        return new AggregatesStoreImpl<>(builder.build());
    }
}
