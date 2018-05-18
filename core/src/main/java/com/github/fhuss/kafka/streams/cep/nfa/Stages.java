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
package com.github.fhuss.kafka.streams.cep.nfa;

import com.github.fhuss.kafka.streams.cep.pattern.StateAggregator;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A complex pattern sequence is made of multiple {@link Stage}.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class Stages<K, V> implements Iterable<Stage<K, V>> {

    private final List<Stage<K, V>> stages;

    /**
     * Creates a new {@link Stages} instance.
     *
     * @param stages    the list of stages.
     */
    public Stages(final List<Stage<K, V>> stages) {
        this.stages = stages;
    }

    public List<Stage<K, V>> getAllStages() {
        return stages;
    }

    public Stage<K, V> getBeginingStage() {
        return stages.stream().filter(Stage::isBeginState).findFirst().get();
    }

    public ComputationStage<K, V> initialComputationStage() {
        ComputationStage<K, V> computation = new ComputationStageBuilder<K, V>()
                .setStage(getBeginingStage())
                .setVersion(new DeweyVersion(1))
                .setSequence(1L)
                .build();
        return computation;
    }

    public Set<String> getDefinedStates() {
        return stages.stream()
                .flatMap(s -> s.getAggregates().stream())
                .map(StateAggregator::getName)
                .collect(Collectors.toSet());
    }

    @Override
    public Iterator<Stage<K, V>> iterator() {
        return stages.iterator();
    }
}
