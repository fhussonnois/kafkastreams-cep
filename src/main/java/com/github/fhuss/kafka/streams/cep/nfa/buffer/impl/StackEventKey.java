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
package com.github.fhuss.kafka.streams.cep.nfa.buffer.impl;

import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import org.apache.commons.lang3.builder.CompareToBuilder;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class is used to uniquely identify a kafka message that matched a specific state.
 */
public class StackEventKey implements Serializable, Comparable<StackEventKey>{

    private StateKey state;
    private String topic;
    private int partition;
    private long offset;


    /**
     * Dummy constructor required by Kryo.
     */
    public StackEventKey() {}

    /**
     * Creates a new {@link StackEventKey} instance.
     *
     * @param state the state.
     * @param topic the name of the topic.
     * @param partition the partition of the message.
     * @param offset the offset of the message.
     */
    StackEventKey(StateKey state, String topic, int partition, long offset) {
        this.state = state;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    StateKey getState() {
        return state;
    }

    String getTopic() {
        return topic;
    }

    int getPartition() {
        return partition;
    }

    long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StackEventKey that = (StackEventKey) o;
        return partition == that.partition &&
                offset == that.offset &&
                Objects.equals(state, that.state) &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, topic, partition, offset);
    }

    @Override
    public int compareTo(StackEventKey that) {

        CompareToBuilder compareToBuilder = new CompareToBuilder();
        return compareToBuilder.append(this.topic, that.topic)
                .append(this.partition, that.partition)
                .append(this.offset, that.offset)
                .append(this.state, that.state)
                .build();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StackEventKey{");
        sb.append("state=").append(state);
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", partition=").append(partition);
        sb.append(", offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }

    static class StateKey implements Serializable, Comparable<StateKey> {

        private String name;
        private Stage.StateType type;

        public StateKey(){}

        StateKey(String name, Stage.StateType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public Stage.StateType getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StateKey stateKey = (StateKey) o;
            return Objects.equals(name, stateKey.name) &&
                    type == stateKey.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }

        @Override
        public int compareTo(StateKey o) {
            return this.name.compareTo(o.name);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("StateKey{");
            sb.append("name='").append(name).append('\'');
            sb.append(", type=").append(type);
            sb.append('}');
            return sb.toString();
        }
    }
}