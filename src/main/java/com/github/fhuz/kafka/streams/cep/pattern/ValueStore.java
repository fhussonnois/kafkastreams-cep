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
package com.github.fhuz.kafka.streams.cep.pattern;

import org.apache.kafka.streams.state.KeyValueStore;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Simple class to wrap a {@link KeyValueStore}.
 */
public class ValueStore<V> {

    private final KeyValueStore<SequenceStateKey, V> backedStore;

    private final SequenceStateKey key;

    /**
     * Creates a new {@link ValueStore} instance.
     * @param topic
     * @param partition
     * @param version
     * @param backedStore
     */
    public ValueStore(String topic, int partition, UUID version, KeyValueStore<SequenceStateKey, V> backedStore) {
        this.backedStore = backedStore;
        this.key = new SequenceStateKey(topic, partition, version.toString());
    }

    /**
     * @see {@link KeyValueStore#get(Object)}
     */
    public V get() {
        return backedStore.get(this.key);
    }
    /**
     * @see {@link KeyValueStore#put(Object, Object)}.
     */
    public void set(V value) {
        backedStore.put(this.key, value);
    }
    /**
     * @see {@link KeyValueStore#putIfAbsent(Object, Object)}.
     */
    public V setIfAbsent(V value) {
        return backedStore.putIfAbsent(this.key, value);
    }

    /**
     * @see {@link KeyValueStore#delete(Object)}.
     */
    public V delete() {
        return backedStore.delete(this.key);
    }

    /**
     * @see {@link KeyValueStore#name()}.
     */
    public String name() {
        return backedStore.name();
    }
    /**
     * @see {@link KeyValueStore#persistent()}.
     */
    public boolean persistent() {
        return backedStore.persistent();
    }

    /**
     * Duplicates the underlying state for the specified sequence.
     * @param newSeqId the new sequence identifier for which this state will be duplicate.
     * @return a new {@link ValueStore}.
     */
    @SuppressWarnings("unchecked")
    public ValueStore branch(UUID newSeqId) {
        V o = get();
        if( o != null )
            backedStore.put(new SequenceStateKey(key.topic, key.partition, newSeqId.toString()), o);
        return new ValueStore(this.key.topic, this.key.partition, newSeqId, this.backedStore);
    }

    private static class SequenceStateKey implements Comparable<SequenceStateKey>, Serializable {

        public String topic;
        public int partition;
        public String version;

        /**
         * Dummy constructor for serialization.
         */
        public SequenceStateKey(){}

        public SequenceStateKey(String topic, int partition, String version) {
            this.topic = topic;
            this.partition = partition;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SequenceStateKey that = (SequenceStateKey) o;
            return partition == that.partition &&
                    Objects.equals(version, that.version) &&
                    Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, version);
        }

        @Override
        public int compareTo(SequenceStateKey that) {
            if( !this.topic.equals(that.topic) || this.partition != that.partition)
                throw new IllegalArgumentException("Cannot compare event from different topic/partition");

            if(this.partition > that.partition) return 1;
            else if (this.partition < that.partition) return -1;
            else return 0;
        }
    }
}
