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
package com.github.fhuss.kafka.streams.cep.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.SharedVersionedBuffer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * {@link Serde} used to serialize and deserialize data into {@link SharedVersionedBuffer}.
 * @param <T> type of data.
 */
public class KryoSerDe<T> implements Serde<T> {

    private KryoPool pool;

    private KryoSerDeserializer<T> kryoSerDeserializer;

    public KryoSerDe() {
        init();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        init();
    }

    private void init() {
        if (this.pool == null) {
            this.pool = new KryoPool.Builder(Kryo::new).build();
            this.kryoSerDeserializer = new KryoSerDeserializer<>(pool);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<T> serializer() {
        return kryoSerDeserializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return new KryoSerDeserializer<>(pool);
    }

    private static class KryoSerDeserializer<T> implements Serializer<T>, Deserializer<T> {

        private KryoPool kryoPool;

        KryoSerDeserializer(KryoPool kryoPool) {
            this.kryoPool = kryoPool;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        @SuppressWarnings("unchecked")
        public T deserialize(String topic, byte[] data) {
            if( data == null) return null;

            Kryo kryo = kryoPool.borrow();
            ByteBufferInputStream bbis = new ByteBufferInputStream(ByteBuffer.wrap(data));
            Input input = new Input(bbis);
            T result = (T) kryo.readClassAndObject(input);
            this.kryoPool.release(kryo);

            return result;
        }

        @Override
        public byte[] serialize(String topic, T data) {
            Kryo kryo = kryoPool.borrow();
            ByteArrayOutputStream basos = new ByteArrayOutputStream();
            Output output = new Output(basos);
            kryo.writeClassAndObject(output, data);
            output.flush();
            this.kryoPool.release(kryo);
            return basos.toByteArray();
        }

        @Override
        public void close() {

        }
    }
}
