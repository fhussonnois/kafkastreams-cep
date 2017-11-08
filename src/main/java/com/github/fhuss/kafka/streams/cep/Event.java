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
package com.github.fhuss.kafka.streams.cep;

import java.util.Objects;

/**
 * Class to wrap a message a uniquely identifiable kafka message.
 *
 * @param <K> the type of the key event.
 * @param <V> the type of the value event.
 */
public class Event<K, V> implements Comparable<Event<K, V>> {

    public final K key;

    public final V value;

    public final long timestamp;

    public final String topic;

    public final int partition;

    public long offset;

    /**
     * Creates a new {@link Event} instance.
     *
     * @param key the key for the message
     * @param value the value for the message
     */
    public Event(K key, V value, long timestamp, String topic, int partition, long offset) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event<?, ?> event = (Event<?, ?>) o;
        return partition == event.partition &&
                offset == event.offset &&
                Objects.equals(topic, event.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Event{");
        sb.append("key=").append(key);
        sb.append(", value=").append(value);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", partition=").append(partition);
        sb.append(", offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int compareTo(Event<K, V> that) {
        if( !this.topic.equals(that.topic) || this.partition != that.partition)
            return new Long(this.timestamp).compareTo(that.timestamp);

        if(this.offset > that.offset) return 1;
        else if (this.offset < that.offset) return -1;
        else return 0;
    }
}
