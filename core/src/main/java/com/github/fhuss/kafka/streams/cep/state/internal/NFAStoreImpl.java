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
package com.github.fhuss.kafka.streams.cep.state.internal;

import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.state.NFAStore;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.ComputationStageSerde;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.NFAStateValueSerde;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.RunnedKeySerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;

public class NFAStoreImpl<K, V> extends WrappedStateStore.AbstractStateStore implements NFAStore<K, V> {

    private final KeyValueStore<Bytes, byte[]> bytesStore;

    private final List<Stage<K, V>> stages;

    private String topic;

    private StateSerdes<Runned<K>, NFAStates<K, V>> serdes;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    /**
     * Creates a new {@link NFAStoreImpl} instance.
     * @param bytesStore
     * @param stages
     * @param keySerde
     * @param valueSerde
     */
    public NFAStoreImpl(final KeyValueStore<Bytes, byte[]> bytesStore,
                        final List<Stage<K, V>> stages,
                        final Serde<K> keySerde,
                        final Serde<V> valueSerde)  {
        super(bytesStore);
        this.stages = stages;
        this.bytesStore = bytesStore;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public void init(final ProcessorContext context, final StateStore root) {
        final String storeName = bytesStore.name();
        topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        bytesStore.init(context, root);

        Serde<K> kSerde = keySerde == null ? (Serde<K>) context.keySerde() : keySerde;
        Serde<NFAStates<K, V>> serdes = new NFAStateValueSerde<>(
                new ComputationStageSerde<>(stages, kSerde, valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde));

        this.serdes = new StateSerdes<>(topic, new RunnedKeySerde<>(kSerde), serdes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final Runned key, final NFAStates state) {
        bytesStore.put(Bytes.wrap(serdes.rawKey(key)), serdes.rawValue(state));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NFAStates<K, V> find(final Runned key) {
        byte[] bytes = bytesStore.get(Bytes.wrap(serdes.rawKey(key)));
        return serdes.valueFrom(bytes);
    }
}
