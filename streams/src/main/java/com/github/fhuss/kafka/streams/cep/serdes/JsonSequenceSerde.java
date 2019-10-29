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
package com.github.fhuss.kafka.streams.cep.serdes;

import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSequenceSerde<K, V>  implements Serde<Sequence<K, V>> {

    private static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Sequence<K, V>> serializer() {
        return new SequenceSerializer<>();
    }

    @Override
    public Deserializer<Sequence<K, V>> deserializer() {
        return new SequenceDeserializer<>();
    }

    public static class SequenceSerializer<K, V> implements Serializer<Sequence<K, V>>{

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public byte[] serialize(final String s, final Sequence<K, V> sequence) {
            Gson gson = new Gson();
            return gson.toJson(sequence).getBytes(CHARSET_UTF8);
        }

        @Override
        public void close() {

        }
    }

    public static class SequenceDeserializer<K, V> implements Deserializer<Sequence<K, V>>{

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public Sequence<K, V> deserialize(String s, byte[] bytes) {
            return new Gson().fromJson(new String(bytes, CHARSET_UTF8), Sequence.class);
        }

        @Override
        public void close() {

        }
    }
}
