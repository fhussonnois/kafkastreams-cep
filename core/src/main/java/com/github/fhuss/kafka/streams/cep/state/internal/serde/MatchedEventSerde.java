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
package com.github.fhuss.kafka.streams.cep.state.internal.serde;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.fhuss.kafka.streams.cep.state.internal.MatchedEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MatchedEventSerde<K, V>  implements Serde<MatchedEvent<K, V>>  {

    private MatchedEventSerdes serdes;

    /**
     * Creates a new {@link MatchedEventSerde} instance.
     */
    public MatchedEventSerde(final Serde<K> keys, final Serde<V> values) {
        this.serdes = new MatchedEventSerdes(keys, values);
    }

    @Override
    public void configure(final Map<String, ?> map, final boolean isKey) {

    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<MatchedEvent<K, V>> serializer() {
        return serdes;
    }

    @Override
    public Deserializer<MatchedEvent<K, V>> deserializer() {
        return serdes;
    }

    private class MatchedEventSerdes extends AbstractKryoSerde<MatchedEvent<K, V>, K, V> {

        /**
         * Creates a new {@link AbstractKryoSerde} instance.
         *
         * @param keys   Serde used for key.
         * @param values Serde used for value.
         */
        public MatchedEventSerdes(Serde<K> keys, Serde<V> values) {
            super(keys, values);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SuppressWarnings("unchecked")
        protected MatchedEvent<K, V> deserialize(final String topic, final Input input) {
            long timestamp = input.readLong();
            long refs = input.readLong();
            Collection<MatchedEvent.Pointer> processors = (Collection<MatchedEvent.Pointer>)kryo.readClassAndObject(input);
            int keyBytesSize = input.readInt();
            K key = (keyBytesSize > 0) ? keys.deserializer().deserialize(topic, input.readBytes(keyBytesSize)) : null;
            int valueBytesSize = input.readInt();
            V value = (valueBytesSize > 0) ? values.deserializer().deserialize(topic, input.readBytes(valueBytesSize)) : null;
            return new MatchedEvent<>(timestamp, key, value, new AtomicLong(refs), processors);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void serialize(String topic, MatchedEvent<K, V> data, Output output) {
            output.writeLong(data.getTimestamp());
            output.writeLong(data.getRefs().longValue());
            kryo.writeClassAndObject(output, data.getPredecessors());
            write(topic, keys.serializer(), data.getKey(), output);
            write(topic, values.serializer(), data.getValue(), output);
        }

        private <T> void write(String topic, Serializer<T> ser, T value, Output output) {
            if( value != null) {
                byte[] valueBytes = ser.serialize(topic, value);
                output.writeInt(valueBytes.length);
                output.write(valueBytes);
            } else {
                output.writeInt(0);
            }
        }
    }
}
