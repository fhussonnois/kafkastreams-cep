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

import com.github.fhuss.kafka.streams.cep.core.nfa.ComputationStage;
import com.github.fhuss.kafka.streams.cep.core.state.internal.NFAStates;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

public class NFAStateValueSerde<K, V> implements Serde<NFAStates<K, V>> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private ComputationStageSerde<K, V> computationStageSerde;

    /**
     * Creates a new {@link NFAStateValueSerde} instance.
     * @param computationStageSerde the serde instance used for {@link ComputationStage}.
     */
    public NFAStateValueSerde(final ComputationStageSerde<K, V> computationStageSerde) {
        Objects.requireNonNull(computationStageSerde, "computationStageSerde cannot be null");
        this.computationStageSerde = computationStageSerde;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<NFAStates<K, V>> serializer() {
        return new NFAStateValueSerializer();
    }

    @Override
    public Deserializer<NFAStates<K, V>> deserializer() {
        return new NFAStateValueDeserializer();
    }


    private class NFAStateValueSerializer implements Serializer<NFAStates<K, V>> {

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public byte[] serialize(String topic, NFAStates<K, V> data) {

            int offsets = data.getLatestOffsets().size();
            int size = data.getLatestOffsets().entrySet()
                    .stream()
                    .map(e -> e.getKey().getBytes(CHARSET).length)
                    .reduce(0, (v1, v2) -> v1 + v2);

            int capacity = Long.BYTES * (offsets + 1) + Integer.BYTES * (offsets + 1) + size ;
            ByteBuffer buffer = ByteBuffer.allocate(capacity);
            buffer.putInt(data.getLatestOffsets().size());
            for(Map.Entry<String, Long> entry : data.getLatestOffsets().entrySet()) {
                byte[] bytes = entry.getKey().getBytes(CHARSET);
                buffer.putLong(entry.getValue());
                buffer.putInt(bytes.length);
                buffer.put(bytes);
            }
            buffer.putLong(data.getRuns());
            byte[] offsetBytes = buffer.array();

            ByteBuffer capacityBuffer =  ByteBuffer.allocate(Integer.BYTES);
            capacityBuffer.putInt(capacity);

            byte[] capacityBytes = capacityBuffer.array();

            byte[] stagesBytes = computationStageSerde.serialize(topic, data.getComputationStages());

            byte[] bytes = new byte[capacityBytes.length + stagesBytes.length + offsetBytes.length];
            System.arraycopy(capacityBytes, 0, bytes, 0, capacityBytes.length);
            System.arraycopy(stagesBytes, 0, bytes, capacityBytes.length, stagesBytes.length);
            System.arraycopy(offsetBytes, 0, bytes, capacityBytes.length + stagesBytes.length, offsetBytes.length);
            return bytes;
        }

        @Override
        public void close() {

        }
    }

    private class NFAStateValueDeserializer implements Deserializer<NFAStates<K, V>> {

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public NFAStates<K, V> deserialize(String topic, byte[] bytes) {
            if (bytes == null) return null;

            ByteBuffer b = ByteBuffer.allocate(bytes.length).put(bytes);
            b.flip();

            int capacity = b.getInt();

            byte[] computationBytes = Arrays.copyOfRange(bytes, Integer.BYTES, bytes.length);
            Queue<ComputationStage<K, V>> queue = computationStageSerde.deserialize(topic, computationBytes);

            ByteBuffer buffer = ByteBuffer.allocate(capacity);
            buffer.put(Arrays.copyOfRange(bytes, bytes.length - capacity, bytes.length));
            buffer.flip();

            int offsets = buffer.getInt();
            Map<String, Long> latestOffsets = new HashMap<>();
            for (int i = 0; i < offsets; i ++) {
                deserializeAndPutTopicAndOffset(buffer, latestOffsets);
            }
            long runs   = buffer.getLong();
            return new NFAStates<>(queue, runs, latestOffsets);
        }

        private void deserializeAndPutTopicAndOffset(final ByteBuffer buffer, final Map<String, Long> latestOffsets) {
            long offset = buffer.getLong();
            int length = buffer.getInt() ;
            byte[] topicBytes = new byte[length];
            buffer.get(topicBytes);
            latestOffsets.put(new String(topicBytes, CHARSET), offset);
        }

        @Override
        public void close() {

        }

    }
}