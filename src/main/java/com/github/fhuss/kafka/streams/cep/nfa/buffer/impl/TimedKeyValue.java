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

import com.github.fhuss.kafka.streams.cep.nfa.DeweyVersion;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class TimedKeyValue<K, V> implements Serializable, Comparable<TimedKeyValue<K, V>> {
    private long timestamp;
    private K key;
    private V value;
    private AtomicLong refs;

    private Collection<Pointer> predecessors;

    TimedKeyValue(K key, V value, long timestamp) {
        this(timestamp, key, value, new AtomicLong(1), null);
    }

    TimedKeyValue(long timestamp, K key, V value, AtomicLong refs, Collection<Pointer> predecessors) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.refs = refs;
        this.predecessors = predecessors;
    }

    AtomicLong getRefs() {
        return refs;
    }

    public void setRef(long ref) {
        this.refs.set(ref);
    }

    long incrementRefAndGet() {
        return this.refs.incrementAndGet();
    }

    long decrementRefAndGet() {
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

    void removePredecessor(Pointer pointer) {
        this.predecessors.remove(pointer);
    }

    Collection<Pointer> getPredecessors() {
        return predecessors;
    }

    Pointer getPointerByVersion(DeweyVersion version) {
        Pointer ret = null;
        for(Pointer p : predecessors) {
            if(version.isCompatible(p.version)) {
                ret = p;
                break;
            }
        }
        return ret;
    }

    void addPredecessor(DeweyVersion version, StackEventKey key) {
        if( predecessors == null )
            predecessors = new ArrayList<>();
        predecessors.add(new Pointer(version, key));
    }

    @Override
    public int compareTo(TimedKeyValue<K, V> that) {
        return new Long(this.timestamp).compareTo(that.getTimestamp());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TimedKeyValue{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", key=").append(key);
        sb.append(", value=").append(value);
        sb.append(", refs=").append(refs);
        sb.append(", predecessors=").append(predecessors);
        sb.append('}');
        return sb.toString();
    }

    static class Pointer implements Serializable {
        private StackEventKey key;
        private DeweyVersion version;

        /**
         * Dummy constructor required by Kryo.
         */
        public Pointer() {}

        Pointer(DeweyVersion version, StackEventKey key) {
            this.version = version;
            this.key = key;
        }

        StackEventKey getKey() {
            return key;
        }

        DeweyVersion getVersion() {
            return version;
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
}
