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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.fhuss.kafka.streams.cep.serde.AbstractKryoSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class TimedKeyValueSerDes<K, V> extends AbstractKryoSerde<TimedKeyValue<K, V>, K, V> {

    /**
     * Creates a new {@link TimedKeyValueSerDes} instance.
     */
    public TimedKeyValueSerDes(Serde<K> keys, Serde<V> values) {
        super(keys, values);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    protected TimedKeyValue<K, V> deserialize(String topic, Input input) {
        long timestamp = input.readLong();
        long refs = input.readLong();
        Collection<TimedKeyValue.Pointer> processors = (Collection<TimedKeyValue.Pointer>)kryo.readClassAndObject(input);
        int keyBytesSize = input.readInt();
        K key = (keyBytesSize > 0) ? keys.deserializer().deserialize(topic, input.readBytes(keyBytesSize)) : null;
        int valueBytesSize = input.readInt();
        V value = (valueBytesSize > 0) ? values.deserializer().deserialize(topic, input.readBytes(valueBytesSize)) : null;
        return new TimedKeyValue<>(timestamp, key, value, new AtomicLong(refs), processors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void serialize(String topic, TimedKeyValue<K, V> data, Output output) {
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

    @Override
    public void close() {
    }
}
