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
package com.github.fhuss.kafka.streams.cep.state.internal.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public abstract class AbstractKryoSerde <T, K, V>  implements Serializer<T>, Deserializer<T> {

    protected Kryo kryo;
    protected Serde<K> keys;
    protected Serde<V> values;

    /**
     * Creates a new {@link AbstractKryoSerde} instance.
     * @param keys Serde used for key.
     * @param values Serde used for value.
     */
    AbstractKryoSerde(final Serde<K> keys, final Serde<V> values) {
        if (keys == null) throw new IllegalArgumentException("keys null");
        if (values == null) throw new IllegalArgumentException("values null");
        this.keys = keys;
        this.values = values;
        this.kryo = new Kryo();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try (Input input = new Input(new ByteBufferInputStream(ByteBuffer.wrap(data)))) {
            return deserialize(topic, input);
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        ByteArrayOutputStream basos = new ByteArrayOutputStream();
        Output output = new Output(basos);
        serialize(topic, data, output);
        output.flush();
        return basos.toByteArray();
    }

    protected abstract T deserialize(String topic, Input input) ;

    protected abstract void serialize(String topic, T data, Output output);

    @Override
    public void close() {

    }
}
