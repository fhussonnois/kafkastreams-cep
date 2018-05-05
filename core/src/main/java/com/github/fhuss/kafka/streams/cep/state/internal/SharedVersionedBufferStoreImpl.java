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

import com.github.fhuss.kafka.streams.cep.Event;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStore;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.KryoSerDe;
import com.github.fhuss.kafka.streams.cep.state.internal.serde.MatchedEventSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shared version buffer implementation based on Kafka Streams {@link KeyValueStore}.
 *
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 */
public class SharedVersionedBufferStoreImpl<K , V>  extends WrappedStateStore.AbstractStateStore implements SharedVersionedBufferStore<K, V> {

    private static Logger LOG = LoggerFactory.getLogger(SharedVersionedBufferStoreImpl.class);

    private KeyValueStore<Bytes, byte[]> bytesStore;

    private StateSerdes<Matched, MatchedEvent<K, V>> serdes;

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private String topic;

    /**
     * Creates a new {@link SharedVersionedBufferStoreImpl} instance.
     *
     * @param bytesStore the kafka processor context.
     */
    @SuppressWarnings("unchecked")
    public SharedVersionedBufferStoreImpl(
            final KeyValueStore<Bytes, byte[]> bytesStore,
            final Serde<K> keySerde,
            final Serde<V> valueSerde)  {
        super(bytesStore);
        this.bytesStore = bytesStore;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        final String storeName = bytesStore.name();
        topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);

        final Serde<MatchedEvent<K, V>> valueSerDes = new MatchedEventSerde<>(
                keySerde == null ?  (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.serdes = new StateSerdes<>(topic, new KryoSerDe<Matched>(), valueSerDes);

        bytesStore.init(context, root);
    }

    /**
     * Add a new event into the shared buffer.
     *
     * @param currStage the state for which the event must be added.
     * @param currEvent the current event to add.
     * @param prevStage the predecessor state.
     * @param prevEvent the predecessor event.
     * @param version the predecessor version.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void put(final Stage<K, V> currStage,
                    final Event<K, V> currEvent,
                    final Stage<K, V> prevStage,
                    final Event<K, V> prevEvent,
                    final DeweyVersion version) {

        Matched prevEventKey = Matched.from(prevStage, prevEvent);
        Matched currEventKey = Matched.from(currStage, currEvent);

        byte[] prevBytes = this.bytesStore.get(Bytes.wrap(serdes.rawKey(prevEventKey)));
        MatchedEvent sharedPrevEvent = serdes.valueFrom(prevBytes);

        if (sharedPrevEvent == null) {
            throw new IllegalStateException("Cannot find predecessor event for " + prevEventKey);
        }

        byte[] currBytes = this.bytesStore.get(Bytes.wrap(serdes.rawKey(currEventKey)));
        MatchedEvent sharedCurrEvent = serdes.valueFrom(currBytes);

        if (sharedCurrEvent == null) {
            sharedCurrEvent = new MatchedEvent<>(currEvent.key(), currEvent.value(), currEvent.timestamp());
        }
        sharedCurrEvent.addPredecessor(version, prevEventKey);
        LOG.debug("Putting event to store with key={}, value={}", currEventKey, sharedCurrEvent);
        this.bytesStore.put(Bytes.wrap(serdes.rawKey(currEventKey)), serdes.rawValue(sharedCurrEvent));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void branch(final Stage<K, V> stage, final Event<K, V> event, final DeweyVersion version) {
        Matched key = Matched.from(stage, event);
        MatchedEvent.Pointer pointer = new MatchedEvent.Pointer(version, key);
        while(pointer != null && (key = pointer.getKey()) != null) {
            byte[] bytes = this.bytesStore.get(Bytes.wrap(serdes.rawKey(key)));
            final MatchedEvent<K, V> val = serdes.valueFrom(bytes);
            val.incrementRefAndGet();
            this.bytesStore.put(Bytes.wrap(serdes.rawKey(key)), serdes.rawValue(val));
            pointer = val.getPointerByVersion(pointer.getVersion());
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void put(final Stage<K, V> stage, final Event<K, V> event, final DeweyVersion version) {
        // A MatchedEvent can only by add once to a stack, so there is no need to check for existence.
        MatchedEvent<K, V> value = new MatchedEvent<>(event.key(), event.value(), event.timestamp());
        value.addPredecessor(version, null); // register an empty predecessor to kept track of the version (akka run).

        final Matched matched = new Matched(stage.getName(), stage.getType(), event.topic(), event.partition(), event.offset());
        LOG.debug("Putting event to store with key={}, value={}", matched, value);
        this.bytesStore.put(Bytes.wrap(serdes.rawKey(matched)), serdes.rawValue(value));
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Sequence<K, V> get(final Matched matched, final DeweyVersion version) {
        return peek(matched, version, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sequence<K, V> remove(final Matched matched, final DeweyVersion version) {
        return peek(matched, version, true);
    }

    private Sequence<K, V> peek(final Matched matched, DeweyVersion version, boolean remove) {
        MatchedEvent.Pointer pointer = new MatchedEvent.Pointer(version, matched);

        Sequence.Builder<K, V> builder = new Sequence.Builder<>();

        while (pointer != null && pointer.getKey() != null) {
            final Matched key = pointer.getKey();
            byte[] bytes = this.bytesStore.get(Bytes.wrap(serdes.rawKey(key)));
            final MatchedEvent<K, V> stateValue = serdes.valueFrom(bytes);

            long refsLeft = stateValue.decrementRefAndGet();
            if (remove && refsLeft == 0 && stateValue.getPredecessors().size() <= 1) {
                this.bytesStore.delete(Bytes.wrap(serdes.rawKey(key)));
            }

            builder.add(key.getStageName(), newEvent(key, stateValue));
            pointer = stateValue.getPointerByVersion(pointer.getVersion());

            if (remove && pointer != null && refsLeft == 0) {
                stateValue.removePredecessor(pointer);
                this.bytesStore.put(Bytes.wrap(serdes.rawKey(key)), serdes.rawValue(stateValue));

            }
        }
        return builder.build(true);
    }

    private Event<K, V> newEvent(final Matched stateKey, final MatchedEvent<K, V> stateValue) {
        return new Event<>(
                stateValue.getKey(),
                stateValue.getValue(),
                stateValue.getTimestamp(),
                stateKey.getTopic(),
                stateKey.getPartition(),
                stateKey.getOffset());
    }
}
