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
package com.github.fhuss.kafka.streams.cep.nfa;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.fhuss.kafka.streams.cep.Event;
import com.github.fhuss.kafka.streams.cep.serde.AbstractKryoSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;


public class ComputationStageSerDe<K, V>  extends AbstractKryoSerde<Queue<ComputationStage<K, V>>, K, V> {

    private Map<String, Stage<K, V>> stagesKeyedByNames;

    /**
     * Creates a new {@link ComputationStageSerDe} instance.
     */
    public ComputationStageSerDe(List<Stage<K, V>> stages, Serde<K> keys, Serde<V> values) {
        super(keys, values);
        this.stagesKeyedByNames = new HashMap<>();
        for(Stage<K, V> stage : stages) {
            stagesKeyedByNames.put(stage.getName(), stage);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Queue<ComputationStage<K, V>> deserialize(String topic, Input input) {
        int size = input.readInt();
        Queue<ComputationStage<K, V>> ol = new LinkedBlockingQueue<>();

        for(int i = 0; i < size ; i++) {

            ComputationStageBuilder<K, V> builder = new ComputationStageBuilder<>();

            builder.setBranching(input.readBoolean())
                   .setSequence(input.readLong())
                   .setTimestamp(input.readLong())
                   .setVersion(new DeweyVersion(input.readString()));

            String stageName     = input.readString();
            boolean isEpsilon    = input.readBoolean();

            Stage<K, V> currentStage = stagesKeyedByNames.get(stageName);

            Event<K, V> event = deserializeEvent(topic, input);

            if( isEpsilon ) {
                String targetStageName = input.readString();
                currentStage = Stage.newEpsilonState(currentStage, stagesKeyedByNames.get(targetStageName));
            }

            ol.add(builder.setEvent(event)
                   .setStage(currentStage)
                   .build());
        }
        return ol;
    }

    private Event<K, V> deserializeEvent(String topic, Input input) {
        boolean hasEvent = input.readBoolean();
        if( hasEvent ) {
            long eventOffset = input.readLong();
            int eventPartition = input.readInt();
            String eventTopic = input.readString();
            long eventTimestamp = input.readLong();

            int keyBytesSize = input.readInt();
            K key = (keyBytesSize > 0) ? keys.deserializer().deserialize(topic, input.readBytes(keyBytesSize)) : null;
            int valueBytesSize = input.readInt();
            V value = (valueBytesSize > 0) ? values.deserializer().deserialize(topic, input.readBytes(valueBytesSize)) : null;

            return new Event<>(key, value, eventTimestamp, eventTopic, eventPartition, eventOffset);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void serialize(String topic, Queue<ComputationStage<K, V>> ol, Output output) {
        output.writeInt(ol.size());
        for(ComputationStage<K, V> data : ol) {

            final Stage<K, V> stage = data.getStage();

            output.writeBoolean(data.isBranching());
            output.writeLong(data.getSequence());
            output.writeLong(data.getTimestamp());
            output.writeString(data.getVersion().toString());
            output.writeString(stage.getName());
            output.writeBoolean(stage.isEpsilonStage());
            serializeEvent(topic, data, output);


            if (stage.isEpsilonStage()) {
                output.writeString(stage.getTargetByOperation(EdgeOperation.PROCEED).getName());
            }
        }
    }

    private void serializeEvent(String topic, ComputationStage<K, V> data, Output output) {
        Event<K, V> event = data.getEvent();
        boolean hasEvent = event != null;
        output.writeBoolean(hasEvent);
        if ( hasEvent ) {
            output.writeLong(event.offset);
            output.writeInt(event.partition);
            output.writeString(event.topic);
            output.writeLong(event.timestamp);
            write(topic, keys.serializer(), event.key, output);
            write(topic, values.serializer(), event.value, output);
        }
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
