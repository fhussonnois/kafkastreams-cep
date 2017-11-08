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
package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.nfa.ComputationStage;
import com.github.fhuss.kafka.streams.cep.nfa.ComputationStageSerDe;
import com.github.fhuss.kafka.streams.cep.processor.internal.NFAStateValue;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;

class NFAStateValueSerDe<K, V> implements Serializer<NFAStateValue<K, V>>, Deserializer<NFAStateValue<K, V>> {

    private ComputationStageSerDe<K, V> computationStageSerDes;

    /**
     * Creates a new {@link NFAStateValueSerDe} instance.
     *
     * @param computationStageSerDes
     */
    NFAStateValueSerDe(ComputationStageSerDe<K, V> computationStageSerDes) {
        this.computationStageSerDes = computationStageSerDes;
    }

    @Override
    public NFAStateValue<K, V> deserialize(String topic, byte[] bytes) {
        if (bytes == null) return null;
        Queue<ComputationStage<K, V>> queue = computationStageSerDes.deserialize(topic, bytes);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.put(Arrays.copyOfRange(bytes, bytes.length - Long.BYTES * 2, bytes.length));
        buffer.flip();
        long offset = buffer.getLong();
        long runs   = buffer.getLong();
        return new NFAStateValue<>(queue, runs, offset);
    }

    @Override
    public byte[] serialize(String topic, NFAStateValue<K, V> data) {
        byte[] stagesBytes = computationStageSerDes.serialize(topic, data.getComputationStages());
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(data.getLatestOffset());
        buffer.putLong(data.getRuns());
        byte[] offsetBytes = buffer.array();

        byte[] bytes = new byte[stagesBytes.length + offsetBytes.length];
        System.arraycopy(stagesBytes, 0, bytes, 0, stagesBytes.length);
        System.arraycopy(offsetBytes, 0, bytes, stagesBytes.length, offsetBytes.length);
        return bytes;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }
    @Override
    public void close() {

    }
}