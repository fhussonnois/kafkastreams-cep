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

import com.github.fhuss.kafka.streams.cep.core.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.core.state.NFAStore;
import com.github.fhuss.kafka.streams.cep.state.NFAStateStore;
import com.github.fhuss.kafka.streams.cep.state.internal.NFAStoreImpl;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.List;

/**
 * Default class to build {@link NFAStore} instance.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class NFAStoreBuilder<K, V> extends AbstractStoreBuilder<K, V, NFAStateStore<K, V>> {

    private final KeyValueBytesStoreSupplier storeSupplier;

    private final List<Stage<K, V>> stages;

    public NFAStoreBuilder(final KeyValueBytesStoreSupplier storeSupplier,
                           final List<Stage<K, V>> stages,
                           final Serde<K> keySerde,
                           final Serde<V> valueSerde) {
        super(storeSupplier.name(), keySerde, valueSerde);
        this.storeSupplier = storeSupplier;
        this.stages = stages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NFAStateStore<K, V> build() {

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
        return new NFAStoreImpl<>(builder.build(), stages, keySerde, valueSerde);
    }
}
