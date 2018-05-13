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
package com.github.fhuss.kafka.streams.cep.state.internal;

import com.github.fhuss.kafka.streams.cep.nfa.ComputationStage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

/**
 * This class is used to wrap the execution state of an {@link com.github.fhuss.kafka.streams.cep.nfa.NFA} instance.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class NFAStates<K, V> implements Serializable {

    private final Queue<ComputationStage<K, V>> computationStages;
    private final Long runs;
    private final Map<String, Long> latestOffsets;

    /**
     * Creates a new {@link NFAStates} instance.
     *
     * @param computationStages     the running computation stages.
     * @param runs                  the sequence number to use for next run.
     */
    public NFAStates(final Queue<ComputationStage<K, V>> computationStages,
                     final Long runs) {
        this(computationStages, runs, new HashMap<>());
    }

    /**
     * Creates a new {@link NFAStates} instance.
     *
     * @param computationStages     the running computation stages.
     * @param runs                  the sequence number to use for next run.
     * @param latestOffsets         the last processed offers per topic.
     */
    public NFAStates(final Queue<ComputationStage<K, V>> computationStages,
                     final Long runs,
                     final Map<String, Long> latestOffsets) {
        this.computationStages = computationStages;
        this.runs = runs;
        this.latestOffsets = latestOffsets;
    }

    public Queue<ComputationStage<K, V>> getComputationStages() {
        return computationStages;
    }

    public Long getRuns() {
        return runs;
    }

    public Map<String, Long> getLatestOffsets() {
        return new HashMap<>(latestOffsets);
    }

    public Long getLatestOffsetForTopic(final String topic) {
        return latestOffsets.get(topic);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return runs.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NFAStates<?, ?> nfaStates = (NFAStates<?, ?>) o;
        return Objects.equals(runs, nfaStates.runs) &&
                Objects.equals(latestOffsets, nfaStates.latestOffsets);
    }

    @Override
    public String toString() {
        return "NFAStates{" +
                "computationStages=" + computationStages +
                ", runs=" + runs +
                ", latestOffsets=" + latestOffsets +
                '}';
    }
}