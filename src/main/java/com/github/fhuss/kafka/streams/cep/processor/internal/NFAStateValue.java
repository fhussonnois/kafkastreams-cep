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
package com.github.fhuss.kafka.streams.cep.processor.internal;

import com.github.fhuss.kafka.streams.cep.nfa.ComputationStage;

import java.io.Serializable;
import java.util.Queue;

public class NFAStateValue<K, V> implements Comparable<NFAStateValue>, Serializable {

    private Queue<ComputationStage<K, V>> computationStages;
    private Long runs;
    private Long latestOffset;

    public NFAStateValue(){}

    /**
     * Creates a new {@link NFAStateValue} instance.
     * @param computationStages
     * @param runs
     * @param latestOffset
     */
    public NFAStateValue(final Queue<ComputationStage<K, V>> computationStages, final Long runs, final Long latestOffset) {
        this.computationStages = computationStages;
        this.runs = runs;
        this.latestOffset = latestOffset;
    }

    public Queue<ComputationStage<K, V>> getComputationStages() {
        return computationStages;
    }

    public Long getRuns() {
        return runs;
    }

    public Long getLatestOffset() {
        return latestOffset;
    }

    @Override
    public int hashCode() {
        return latestOffset.hashCode();
    }

    @Override
    public int compareTo(NFAStateValue that) {
        return this.latestOffset.compareTo(that.latestOffset);
    }
}
