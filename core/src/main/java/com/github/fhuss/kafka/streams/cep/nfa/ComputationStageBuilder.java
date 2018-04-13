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

import com.github.fhuss.kafka.streams.cep.Event;

/**
 * Class to build a new {@link ComputationStage} instance.
 *
 * @param <K>   the record key type
 * @param <V>   the record value type.
 */
public class ComputationStageBuilder<K, V> {

    private Stage<K, V> stage;
    private DeweyVersion version;
    private long sequence;
    private Event<K, V> event       = null;
    private long timestamp          = -1;
    private boolean isBranching     = false;

    public ComputationStageBuilder<K, V> setStage(final Stage<K, V> stage) {
        this.stage = stage;
        return this;
    }

    public ComputationStageBuilder<K, V> setVersion(final DeweyVersion version) {
        this.version = version;
        return this;
    }

    public ComputationStageBuilder<K, V> setSequence(final long sequence) {
        this.sequence = sequence;
        return this;
    }

    public ComputationStageBuilder<K, V> setEvent(final Event<K, V> event) {
        this.event = event;
        return this;
    }

    public ComputationStageBuilder<K, V> setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public ComputationStageBuilder<K, V> setBranching(final boolean isBranching) {
        this.isBranching = isBranching;
        return this;
    }

    public ComputationStage<K, V> build() {
        return new ComputationStage<>(stage, version, event, timestamp, sequence, isBranching);
    }
}