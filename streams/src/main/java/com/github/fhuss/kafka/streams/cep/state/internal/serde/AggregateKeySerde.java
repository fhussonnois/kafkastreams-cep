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

import com.github.fhuss.kafka.streams.cep.core.state.internal.Aggregated;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 */
public class AggregateKeySerde<K> implements Serde<Aggregated<K>> {

    private final Serde<WrappedTopic<K>> keySerde;

    public AggregateKeySerde() {
        this.keySerde = new KryoSerDe<>();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Aggregated<K>> serializer() {
        return new AggregateKeySerializer(keySerde.serializer());
    }

    @Override
    public Deserializer<Aggregated<K>> deserializer() {
        return new AggregateKeyDeserializer(keySerde.deserializer());
    }

    private class AggregateKeySerializer implements Serializer<Aggregated<K>> {

        private final Serializer<WrappedTopic<K>> keySerializer;

        AggregateKeySerializer(final Serializer<WrappedTopic<K>> keySerializer) {
            this.keySerializer = keySerializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {

        }

        @Override
        public byte[] serialize(final String topic, final Aggregated<K> data) {
            if (data == null) {
                return null;
            }
            return keySerializer.serialize(topic, new WrappedTopic<>(topic, data));
        }

        @Override
        public void close() {

        }
    }

    private class AggregateKeyDeserializer implements Deserializer<Aggregated<K>> {
        private final Deserializer<WrappedTopic<K>> deserializer;

        AggregateKeyDeserializer(final Deserializer<WrappedTopic<K>> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
        }

        @Override
        public Aggregated<K> deserialize(final String topic, final byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            WrappedTopic<K> wrapped = deserializer.deserialize(topic, data);
            return wrapped.aggregated;
        }


        @Override
        public void close() {

        }
    }


    private static class WrappedTopic<K> {

        String topic;
        Aggregated<K> aggregated;

       /**
         * Dummy constructor for serialization.
         */
        public WrappedTopic(){}

        public WrappedTopic(final String topic, final Aggregated<K> aggregated) {
            //this.topic = topic;
            this.aggregated = aggregated;
        }
    }

}