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
package com.github.fhuss.kafka.streams.cep.core.state.internal;

import com.github.fhuss.kafka.streams.cep.core.Event;
import com.github.fhuss.kafka.streams.cep.core.nfa.Stage;
import org.apache.commons.lang3.builder.CompareToBuilder;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class is used to uniquely identify a kafka message that matched a specific state.
 */
public class Matched implements Serializable, Comparable<Matched>{

    private String stageName;
    private Stage.StateType stageType;
    private String topic;
    private int partition;
    private long offset;


    /**
     * Dummy constructor required by Kryo.
     */
    public Matched() {}

    /**
     * Creates a new {@link Matched} instance.
     *
     * @param stageName
     * @param stageType
     * @param topic
     * @param partition
     * @param offset
     */
    public Matched(final String stageName,
            final Stage.StateType stageType,
            final String topic,
            final int partition,
            final long offset) {
        this.stageName = stageName;
        this.stageType = stageType;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public static <K, V> Matched from(final Stage<K, V> stage, final Event<K, V> event) {
        return new Matched(stage.getName(), stage.getType(), event.topic(), event.partition(), event.offset());
    }

    public String getStageName() {
        return stageName;
    }

    public Stage.StateType getStageType() {
        return stageType;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Matched that = (Matched) o;
        return partition == that.partition &&
                offset == that.offset &&
                Objects.equals(stageName, that.stageName) &&
                Objects.equals(stageType, that.stageType) &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stageName, stageType, topic, partition, offset);
    }

    @Override
    public int compareTo(Matched that) {

        CompareToBuilder compareToBuilder = new CompareToBuilder();
        return compareToBuilder.append(this.topic, that.topic)
                .append(this.partition, that.partition)
                .append(this.offset, that.offset)
                .append(this.stageName, that.stageName)
                .append(this.stageType, that.stageType)
                .build();
    }

    @Override
    public String toString() {
        return "Matched{" +
                "stageName='" + stageName + '\'' +
                ", stageType=" + stageType +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}