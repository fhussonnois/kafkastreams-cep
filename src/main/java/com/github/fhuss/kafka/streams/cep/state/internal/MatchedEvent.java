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
package com.github.fhuss.kafka.streams.cep.state.internal;

import com.github.fhuss.kafka.streams.cep.nfa.DeweyVersion;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class MatchedEvent<K, V> implements Serializable, Comparable<MatchedEvent<K, V>> {

    private long timestamp;
    private K key;
    private V value;
    private AtomicLong refs;

    private Collection<Pointer> predecessors;

    MatchedEvent(final K key,
                 final V value,
                 final long timestamp) {
        this(timestamp, key, value, new AtomicLong(1), null);
    }

    public MatchedEvent(final long timestamp,
                        final K key,
                        final V value,
                        final AtomicLong refs,
                        final Collection<Pointer> predecessors) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.refs = refs;
        this.predecessors = predecessors;
    }

    public AtomicLong getRefs() {
        return refs;
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

    void removePredecessor(Pointer pointer) {
        this.predecessors.remove(pointer);
    }

    public Collection<Pointer> getPredecessors() {
        return predecessors;
    }

    public Pointer getPointerByVersion(DeweyVersion version) {
        Pointer ret = null;
        for (Pointer p : predecessors) {
            if (version.isCompatible(p.version)) {
                ret = p;
                break;
            }
        }
        return ret;
    }

    void addPredecessor(DeweyVersion version, Matched key) {
        if (predecessors == null )
            predecessors = new ArrayList<>();
        predecessors.add(new Pointer(version, key));
    }

    @Override
    public int compareTo(MatchedEvent<K, V> that) {
        return new Long(this.timestamp).compareTo(that.getTimestamp());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MatchedEvent{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", key=").append(key);
        sb.append(", value=").append(value);
        sb.append(", refs=").append(refs);
        sb.append(", predecessors=").append(predecessors);
        sb.append('}');
        return sb.toString();
    }

    public static class Pointer implements Serializable {

        private Matched key;
        private DeweyVersion version;

        /**
         * Dummy constructor required by Kryo.
         */
        public Pointer() {}

        Pointer(final DeweyVersion version, final Matched key) {
            this.version = version;
            this.key = key;
        }

        Matched getKey() {
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
