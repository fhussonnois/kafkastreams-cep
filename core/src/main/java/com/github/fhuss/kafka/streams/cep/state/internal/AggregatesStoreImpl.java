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

import com.github.fhuss.kafka.streams.cep.state.AggregatesStore;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.AggregateKeySerde;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.KryoSerDe;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;


public class AggregatesStoreImpl<K> extends WrappedStateStore.AbstractStateStore implements AggregatesStore<K> {

    private StateSerdes<Aggregated<K>, Object> serdes;

    private KeyValueStore<Bytes, byte[]> bytesStore;

    private String topic;


    /**
     * Creates a new {@link AggregatesStoreImpl} instance.
     * @param bytesStore
     */
    public AggregatesStoreImpl(final KeyValueStore<Bytes, byte[]> bytesStore) {
        super(bytesStore);
        this.bytesStore = bytesStore;
    }

    public void init(final ProcessorContext context, final StateStore root) {
        final String storeName = bytesStore.name();
        topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        bytesStore.init(context, root);

        serdes = new StateSerdes<>(topic, new AggregateKeySerde<>(), new KryoSerDe<>());
    }

    /**
     * Duplicates the underlying state for the specified sequence.
     */
    @Override
    public void branch(final Aggregated<K> aggregated, long toSequence) {
        Object o = find(aggregated);
        if (o != null) {
            put(new Aggregated<>(aggregated.getKey(), aggregated.getAggregate().setSequence(toSequence)), o);
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T find(Aggregated<K> aggregated) {
        byte[] bytes = bytesStore.get(Bytes.wrap(serdes.rawKey(aggregated)));
        return (T) serdes.valueFrom(bytes);
    }

    @Override
    public <T> void put(Aggregated<K> aggregated, T aggregate) {
        bytesStore.put(Bytes.wrap(serdes.rawKey(aggregated)), serdes.rawValue(aggregate));
    }
}
