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
package com.github.fhuz.kafka.streams.cep.nfa.buffer.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.record.ByteBufferInputStream;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TimedKeyValueSerDes<K, V> implements Serializer<TimedKeyValue<K, V>>, Deserializer<TimedKeyValue<K, V>> {

    private Kryo kryo;

    private Serde<K> keys;

    private Serde<V> values;

    /**
     * Creates a new {@link TimedKeyValueSerDes} instance.
     */
    public TimedKeyValueSerDes(Serde<K> keys, Serde<V> values) {
        this.keys = keys;
        this.values = values;
        this.kryo = new Kryo();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TimedKeyValue<K, V> deserialize(String topic, byte[] data) {
        if( data == null) return null;
        try (Input input = new Input(new ByteBufferInputStream(ByteBuffer.wrap(data)))) {
            long timestamp = input.readLong();
            long refs = input.readLong();
            Collection<TimedKeyValue.Pointer> processors = (Collection<TimedKeyValue.Pointer>)kryo.readClassAndObject(input);
            int keyBytesSize = input.readInt();
            K key = (keyBytesSize > 0) ? keys.deserializer().deserialize(topic, input.readBytes(keyBytesSize)) : null;
            int valueBytesSize = input.readInt();
            V value = (valueBytesSize > 0) ? values.deserializer().deserialize(topic, input.readBytes(valueBytesSize)) : null;
            return new TimedKeyValue<>(timestamp, key, value, new AtomicLong(refs), processors);
        }
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize(String topic, TimedKeyValue<K, V> data) {
        ByteArrayOutputStream basos = new ByteArrayOutputStream();
        Output output = new Output(basos);
        writeData(topic, data, output);
        output.flush();
        return basos.toByteArray();
    }

    private void writeData(String topic, TimedKeyValue<K, V> data, Output output) {
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
