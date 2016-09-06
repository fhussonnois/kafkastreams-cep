/**
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
package com.github.fhuss.kafka.streams.cep.nfa.buffer.impl;

import com.github.fhuss.kafka.streams.cep.Event;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.SharedVersionedBuffer;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.StackEventKey.StateKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A shared version buffer implementation based on Kafka Streams {@link KeyValueStore}.
 *
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 */
public class KVSharedVersionedBuffer<K , V> implements SharedVersionedBuffer<K, V> {

    private KeyValueStore<StackEventKey, TimedKeyValue<K, V>> store;

    public static <K, V> Factory<K, V> getFactory() {
        return new Factory<>();
    }

    public static class Factory<K, V> {

        public KVSharedVersionedBuffer<K, V> make(ProcessorContext context, String storeName) {
            KeyValueStore<StackEventKey, TimedKeyValue<K, V>> store = getStateStore(context, storeName);
            return new KVSharedVersionedBuffer<>(store);
        }

        @SuppressWarnings("unchecked")
        private KeyValueStore<StackEventKey, TimedKeyValue<K, V>> getStateStore(ProcessorContext context, String storeName) {
            StateStore store = context.getStateStore(storeName);
            if(store == null)
                throw new IllegalStateException("No state store registered with name " + storeName);
            return (KeyValueStore<StackEventKey, TimedKeyValue<K, V>>) store;
        }
    }

    /**
     * Creates a new {@link KVSharedVersionedBuffer} instance.
     *
     * @param store the kafka processor context.
     */
    @SuppressWarnings("unchecked")
    public KVSharedVersionedBuffer(KeyValueStore<StackEventKey, TimedKeyValue<K, V>> store) {
        this.store = store;
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
    public void put(Stage<K, V> currStage, Event<K, V> currEvent, Stage<K, V> prevStage, Event<K, V> prevEvent, DeweyVersion version) {
        StateKey currStateKey = new StateKey(currStage.getName(), currStage.getType());
        StateKey prevStateKey = new StateKey(prevStage.getName(), prevStage.getType());
        StackEventKey prevEventKey = new StackEventKey(prevStateKey, prevEvent.topic, prevEvent.partition, prevEvent.offset);
        StackEventKey currEventKey = new StackEventKey(currStateKey, currEvent.topic, currEvent.partition, currEvent.offset);

        TimedKeyValue sharedPrevEvent = this.store.get(prevEventKey);
        if( sharedPrevEvent == null) {
            throw new IllegalStateException("Cannot find predecessor event for " + prevEventKey);
        }

        TimedKeyValue sharedCurrEvent = this.store.get(currEventKey);
        if( sharedCurrEvent == null) {
            sharedCurrEvent = new TimedKeyValue<>(currEvent.key, currEvent.value, currEvent.timestamp);
        }
        sharedCurrEvent.addPredecessor(version, prevEventKey);
        this.store.put(currEventKey, sharedCurrEvent);
    }

    public void branch(Stage<K, V> stage, Event<K, V> event, DeweyVersion version) {
        StackEventKey key = newStackEventKey(stage, event);
        TimedKeyValue.Pointer pointer = new TimedKeyValue.Pointer(version, key);
        while(pointer != null && (key = pointer.getKey()) != null) {
            final TimedKeyValue<K, V> val = this.store.get(key);
            val.incrementRefAndGet();
            if( this.store.persistent() ) {
                this.store.put(key, val);
            }
            pointer = val.getPointerByVersion(pointer.getVersion());
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void put(Stage<K, V> stage, Event<K, V> evt, DeweyVersion version) {

        StateKey stateKey = new StateKey(stage.getName(), stage.getType());

        // A TimedKeyValue can only by add once to a stack, so there is no need to check for existence.
        TimedKeyValue<K, V> eventValue = new TimedKeyValue<>(evt.key, evt.value, evt.timestamp);
        eventValue.addPredecessor(version, null); // register an empty predecessor to kept track of the version (akka run).

        StackEventKey eventKey = new StackEventKey(stateKey, evt.topic, evt.partition, evt.offset);

        this.store.put(eventKey, eventValue);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Sequence<K, V> get(final Stage<K, V> stage, final Event<K, V> event, final DeweyVersion version) {
        return peek(stage, event, version, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sequence<K, V> remove(final Stage<K, V> stage, final Event<K, V> event, final DeweyVersion version) {
        return peek(stage, event, version, true);
    }

    public Sequence<K, V> peek(Stage<K, V> stage, Event<K, V> event, DeweyVersion version, boolean remove) {
        TimedKeyValue.Pointer pointer = new TimedKeyValue.Pointer(version, newStackEventKey(stage, event));

        Sequence<K, V> sequence = new Sequence<>();

        while(pointer != null && pointer.getKey() != null) {
            final StackEventKey stateKey = pointer.getKey();
            final TimedKeyValue<K, V> stateValue = this.store.get(stateKey);

            long refsLeft = stateValue.decrementRefAndGet();
            if (remove && refsLeft == 0 && stateValue.getPredecessors().size() <= 1) {
                store.delete(stateKey);
            }

            sequence.add(stateKey.getState().getName(), newEvent(stateKey, stateValue));
            pointer = stateValue.getPointerByVersion(pointer.getVersion());

            if( remove && pointer != null && refsLeft == 0) {
                stateValue.removePredecessor(pointer);
                if( store.persistent() )
                    store.put(stateKey, stateValue);
            }
        }
        return sequence;
    }

    private StackEventKey newStackEventKey(Stage<K, V> stage, Event<K, V> event) {
        return new StackEventKey(new StateKey(stage.getName(), stage.getType()), event.topic, event.partition, event.offset);
    }

    private Event<K, V> newEvent(StackEventKey stateKey, TimedKeyValue<K, V> stateValue) {
        return new Event<>(
                stateValue.getKey(),
                stateValue.getValue(),
                stateValue.getTimestamp(),
                stateKey.getTopic(),
                stateKey.getPartition(),
                stateKey.getOffset());
    }
}
