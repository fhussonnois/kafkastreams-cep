/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import com.github.fhuss.kafka.streams.cep.core.state.internal.Runned;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 */
public class RunnedKeySerde<K> implements Serde<Runned<K>> {

    private final Serde<K> keySerde;

    public RunnedKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Runned<K>> serializer() {
        return new KeySerializer(keySerde.serializer());
    }

    @Override
    public Deserializer<Runned<K>> deserializer() {
        return new KeyDeserializer(keySerde.deserializer());
    }

    private class KeySerializer implements Serializer<Runned<K>> {

        private final Serializer<K> keySerializer;

        KeySerializer(final Serializer<K> keySerializer) {
            this.keySerializer = keySerializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {

        }

        @Override
        public byte[] serialize(final String topic, final Runned<K> data) {
            if (data == null) {
                return null;
            }
            return keySerializer.serialize(topic, data.getKey());
        }

        @Override
        public void close() {

        }
    }

    private class KeyDeserializer implements Deserializer<Runned<K>> {
        private final Deserializer<K> deserializer;

        KeyDeserializer(final Deserializer<K> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
        }

        @Override
        public Runned<K> deserialize(final String topic, final byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            K value = deserializer.deserialize(topic, data);
            return new Runned<>(value);
        }


        @Override
        public void close() {

        }
    }
}