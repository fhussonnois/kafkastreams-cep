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
package com.github.fhuz.kafka.streams.cep.nfa.buffer;

import com.github.fhuz.kafka.streams.cep.Sequence;
import com.github.fhuz.kafka.streams.cep.nfa.Stage;
import com.github.fhuz.kafka.streams.cep.nfa.DeweyVersion;
import com.github.fhuz.kafka.streams.cep.Event;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A shared version buffer implementation based on Kafka Streams {@link KeyValueStore}.
 *
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 */
public class KVSharedVersionedBuffer<K , V> implements SharedVersionedBuffer<K, V> {

    private KeyValueStore<StackEventKey<K, V>, TimedKeyValue<K, V>> store;

    public static <K, V> Factory<K, V> getFactory() {
        return new Factory<>();
    }

    public static class Factory<K, V> {

        public static final String STATE_NAME = "_cep_sharedbuffer_events";

        public KVSharedVersionedBuffer<K, V> make(ProcessorContext context) {
            KeyValueStore<StackEventKey<K, V>, TimedKeyValue<K, V>> store = getStateStore(context);
            return new KVSharedVersionedBuffer<>(store);
        }

        @SuppressWarnings("unchecked")
        private KeyValueStore<StackEventKey<K, V>, TimedKeyValue<K, V>> getStateStore(ProcessorContext context) {
            StateStore store = context.getStateStore(STATE_NAME);
            if(store == null) {
                throw new IllegalStateException("No state store registered with name " + STATE_NAME);
            }
            return (KeyValueStore<StackEventKey<K, V>, TimedKeyValue<K, V>>) context.getStateStore(STATE_NAME);
        }
    }

    /**
     * Creates a new {@link KVSharedVersionedBuffer} instance.
     *
     * @param store the kafka processor context.
     */
    @SuppressWarnings("unchecked")
    public KVSharedVersionedBuffer(KeyValueStore<StackEventKey<K, V>, TimedKeyValue<K, V>> store) {
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
            throw new IllegalStateException("Cannot find predecessor event for ");
        }

        TimedKeyValue sharedCurrEvent = this.store.get(currEventKey);
        if( sharedCurrEvent == null) {
            sharedCurrEvent = new TimedKeyValue<>(currEvent.key, currEvent.value, currEvent.timestamp);
        }
        sharedCurrEvent.addPredecessor(version, prevEventKey);
        this.store.put(currEventKey, sharedCurrEvent);
    }

    public void branch(Stage<K, V> stage, Event<K, V> event, DeweyVersion version) {
        StackEventKey<K, V> key = newStackEventKey(stage, event);
        Pointer<K, V> pointer = new Pointer<>(version, key);
        while(pointer != null && pointer.key != null) {
            final TimedKeyValue<K, V> val = this.store.get(pointer.key);
            val.incrementRefAndGet();
            if( this.store.persistent() )
                this.store.put(key, val);
            pointer = val.getPointerByVersion(pointer.version);
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
        Pointer<K, V> pointer = new Pointer<>(version, newStackEventKey(stage, event));

        Sequence<K, V> sequence = new Sequence<>();

        while(pointer != null && pointer.key != null) {
            final StackEventKey<K, V> stateKey = pointer.key;
            final TimedKeyValue<K, V> stateValue = this.store.get(stateKey);

            long refsLeft = stateValue.decrementRefAndGet();
            if (remove && refsLeft == 0 && stateValue.getPredecessors().size() <= 1) {
                store.delete(stateKey);
            }

            sequence.add(stateKey.state.name, newEvent(stateKey, stateValue));
            pointer = stateValue.getPointerByVersion(pointer.version);

            if( remove && pointer != null && refsLeft == 0) {
                stateValue.removePredecessor(pointer);
                if( store.persistent() )
                    store.put(stateKey, stateValue);
            }
        }
        return sequence;
    }

    private StackEventKey<K, V> newStackEventKey(Stage<K, V> stage, Event<K, V> event) {
        return new StackEventKey<>(new StateKey(stage.getName(), stage.getType()), event.topic, event.partition, event.offset);
    }

    private Event<K, V> newEvent(StackEventKey<K, V> stateKey, TimedKeyValue<K, V> stateValue) {
        return new Event<>(
                stateValue.key,
                stateValue.value,
                stateValue.timestamp,
                stateKey.topic,
                stateKey.partition,
                stateKey.offset);
    }

    public static class TimedKeyValue<K, V> implements Serializable, Comparable<TimedKeyValue<K, V>> {
        private long timestamp;
        private K key;
        private V value;
        private AtomicLong refs = new AtomicLong(1);

        private Collection<Pointer<K, V>> predecessors;

        /**
         * Dummy constructor required by Kryo.
         */
        public TimedKeyValue() {}

        public TimedKeyValue(K key, V value, long timestamp) {
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
            this.predecessors = null;
        }

        public void setRef(long ref) {
            this.refs.set(ref);
        }

        public long incrementRefAndGet() {
            return this.refs.incrementAndGet();
        }

        public long decrementRefAndGet() {
            return this.refs.get() == 0 ? 0 : this.refs.decrementAndGet();
        }

        public long getTimestamp() {
            return timestamp;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public void removePredecessor(Pointer<K, V> pointer) {
            this.predecessors.remove(pointer);
        }

        public Collection<Pointer<K, V>> getPredecessors() {
            return predecessors;
        }

        public Pointer<K, V> getPointerByVersion(DeweyVersion version) {
            Pointer<K, V> ret = null;
            for(Pointer p : predecessors) {
                if(version.isCompatible(p.version)) {
                    ret = p;
                    break;
                }
            }
            return ret;
        }

        public void addPredecessor(DeweyVersion version, StackEventKey<K, V> key) {
            if( predecessors == null )
                predecessors = new ArrayList<>();
            predecessors.add(new Pointer<>(version, key));
        }

        @Override
        public int compareTo(TimedKeyValue<K, V> that) {
            return new Long(this.timestamp).compareTo(that.getTimestamp());
        }
    }

    public static class Pointer<K, V> implements Serializable {
        private StackEventKey<K, V> key;
        private DeweyVersion version;

        /**
         * Dummy constructor required by Kryo.
         */
        public Pointer() {}

        public Pointer(DeweyVersion version, StackEventKey<K, V> key) {
            this.version = version;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pointer pointer = (Pointer) o;
            return Objects.equals(key, pointer.key) &&
                    Objects.equals(version, pointer.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, version);
        }
    }

    /**
     * This class is used to uniquely identify a kafka message that matched a specific state.
     *
     * @param <K> the type of the message key.
     * @param <V> the type of the message value.
     */
    public static class StackEventKey<K, V> implements Serializable, Comparable<StackEventKey<K, V>>{

        private StateKey state;
        private String topic;
        private int partition;
        private long offset;


        /**
         * Dummy constructor required by Kryo.
         */
        public StackEventKey() {}

        /**
         * Creates a new {@link StackEventKey} instance.
         *
         * @param state the state.
         * @param topic the name of the topic.
         * @param partition the partition of the message.
         * @param offset the offset of the message.
         */
        public StackEventKey(StateKey state, String topic, int partition, long offset) {
            this.state = state;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StackEventKey<?, ?> that = (StackEventKey<?, ?>) o;
            return partition == that.partition &&
                    offset == that.offset &&
                    Objects.equals(state, that.state) &&
                    Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, topic, partition, offset);
        }

        @Override
        public int compareTo(StackEventKey<K, V> that) {
            if( !this.topic.equals(that.topic) || this.partition != that.partition)
                throw new IllegalArgumentException("Cannot compare event from different topic/partition");

            if(this.offset > that.offset) return 1;
            else if (this.offset < that.offset) return -1;
            else return 0;
        }
    }


    public static class StateKey implements Serializable, Comparable<StateKey> {

        private String name;
        private Stage.StateType type;

        public StateKey(){}

        public StateKey(String name, Stage.StateType type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StateKey stateKey = (StateKey) o;
            return Objects.equals(name, stateKey.name) &&
                    type == stateKey.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }

        @Override
        public int compareTo(StateKey o) {
            return this.name.compareTo(o.name);
        }
    }
}
