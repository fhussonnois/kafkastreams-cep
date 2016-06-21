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
package com.github.fhuz.kafka.streams.cep.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.SharedVersionedBuffer;
import org.apache.kafka.common.record.ByteBufferInputStream;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * {@link Serde} used to serialize and deserialize data into {@link SharedVersionedBuffer}.
 * @param <T>
 */
public class KryoSerDe<T> implements Serde<T> {

    private Kryo kryo;


    public KryoSerDe() {
        initKryo();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        initKryo();
    }

    private void initKryo() {
        this.kryo = new Kryo();
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<T> serializer() {
        return new KryoSerDeserializer<>(kryo);
    }

    @Override
    public Deserializer<T> deserializer() {
        return new KryoSerDeserializer<>(kryo);
    }

    public static class KryoSerDeserializer<T> implements Serializer<T>, Deserializer<T> {
        private Kryo kryo;

        public KryoSerDeserializer(Kryo kryo) {
            this.kryo = kryo;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        @SuppressWarnings("unchecked")
        public T deserialize(String topic, byte[] data) {
            ByteBufferInputStream bbis = new ByteBufferInputStream(ByteBuffer.wrap(data));
            Input input = new Input(bbis);
            return  (T)kryo.readClassAndObject(input);
        }

        @Override
        public byte[] serialize(String topic, T data) {
            ByteArrayOutputStream basos = new ByteArrayOutputStream();
            Output output = new Output(basos);
            kryo.writeClassAndObject(output, data);
            output.flush();
            return basos.toByteArray();
        }

        @Override
        public void close() {

        }
    }

}
