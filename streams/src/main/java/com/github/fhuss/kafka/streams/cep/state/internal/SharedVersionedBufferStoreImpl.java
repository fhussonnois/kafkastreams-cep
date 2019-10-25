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

import com.github.fhuss.kafka.streams.cep.core.Event;
import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.core.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.core.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.core.state.internal.DelegateSharedVersionedBufferStore;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Matched;
import com.github.fhuss.kafka.streams.cep.core.state.internal.MatchedEvent;
import com.github.fhuss.kafka.streams.cep.core.state.internal.MatchedEventStore;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStateStore;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.KryoSerDe;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.MatchedEventSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

/**
 * A shared version buffer implementation based on Kafka Streams {@link KeyValueStore}.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class SharedVersionedBufferStoreImpl<K , V> extends WrappedStateStore.AbstractStateStore implements SharedVersionedBufferStateStore<K, V> {

    private KeyValueStore<Bytes, byte[]> bytesStore;

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private DelegateSharedVersionedBufferStore<K, V> delegate;

    /**
     * Creates a new {@link SharedVersionedBufferStoreImpl} instance.
     *
     * @param bytesStore    the {@link KeyValueStore} bytes store.
     * @param keySerde      the {@link Serde} used for key.
     * @param valueSerde    the {@link Serde} used to value.
     */
    public SharedVersionedBufferStoreImpl(
            final KeyValueStore<Bytes, byte[]> bytesStore,
            final Serde<K> keySerde,
            final Serde<V> valueSerde)  {
        super(bytesStore);
        this.bytesStore = bytesStore;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        final String storeName = bytesStore.name();
        String topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);

        final Serde<MatchedEvent<K, V>> valueSerDes = new MatchedEventSerde<>(
                keySerde == null ?  (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        bytesStore.init(context, root);

        final StateSerdes<Matched, MatchedEvent<K, V>> serdes = new StateSerdes<>(topic, new KryoSerDe<>(), valueSerDes);
        final MatchedEventKeyValueStore<K, V> eventStore = new MatchedEventKeyValueStore<>(bytesStore, serdes);
        delegate = new DelegateSharedVersionedBufferStore<>(eventStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final Stage<K, V> currStage,
                    final Event<K, V> currEvent,
                    final Stage<K, V> prevStage,
                    final Event<K, V> prevEvent,
                    final DeweyVersion version) {

        delegate.put(currStage, currEvent, prevStage, prevEvent, version);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void branch(final Stage<K, V> stage, final Event<K, V> event, final DeweyVersion version) {
        delegate.branch(stage, event, version);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final Stage<K, V> stage, final Event<K, V> event, final DeweyVersion version) {
        delegate.put(stage, event, version);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sequence<K, V> get(final Matched matched, final DeweyVersion version) {
        return delegate.get(matched, version);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sequence<K, V> remove(final Matched matched, final DeweyVersion version) {
        return delegate.remove(matched, version);
    }

    private static final class MatchedEventKeyValueStore<K, V> implements MatchedEventStore<K, V> {

        private final KeyValueStore<Bytes, byte[]> bytesStore;
        private StateSerdes<Matched, MatchedEvent<K, V>> serdes;

        MatchedEventKeyValueStore(final KeyValueStore<Bytes, byte[]> bytesStore,
                                  final StateSerdes<Matched, MatchedEvent<K, V>> serdes) {
            this.bytesStore = bytesStore;
            this.serdes = serdes;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public MatchedEvent<K, V> get(final Matched matched) {
            byte[] prevBytes = this.bytesStore.get(Bytes.wrap(serdes.rawKey(matched)));
            return serdes.valueFrom(prevBytes);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void put(final Matched matched, final MatchedEvent<K, V> event) {
            this.bytesStore.put(Bytes.wrap(serdes.rawKey(matched)), serdes.rawValue(event));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void delete(final Matched matched) {
            this.bytesStore.delete(Bytes.wrap(serdes.rawKey(matched)));
        }
    }
}
