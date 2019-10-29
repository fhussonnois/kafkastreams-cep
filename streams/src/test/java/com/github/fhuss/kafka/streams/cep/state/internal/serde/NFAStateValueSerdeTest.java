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
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class NFAStateValueSerdeTest {

    private static final String TOPIC_1 = "topic-1";
    private static final String TOPIC_2 = "topic-2";
    private static final String TOPIC_TEST = "topic-test";

    private ComputationStageSerde<String, String> computationStageSerde;
    private NFAStateValueSerde<String, String> serde;

    @Before
    public void before() {
        this.computationStageSerde = new ComputationStageSerde<>(Collections.emptyList(), Serdes.String(), Serdes.String());
        this.serde = new NFAStateValueSerde<>(computationStageSerde);
    }

    @Test
    public void testSerdeGivenMultipleTopicOffsets() {

        Queue<ComputationStage<String, String>> computationStages = new LinkedBlockingQueue<>();
        Map<String, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(TOPIC_1, 10L);
        latestOffsets.put(TOPIC_2, 5L);

        NFAStates<String, String> states = new NFAStates<>(computationStages, 0L, latestOffsets);

        byte[] bytes = this.serde.serializer().serialize(TOPIC_TEST, states);

        NFAStates<String, String> result = this.serde.deserializer().deserialize(TOPIC_TEST, bytes);
        Assert.assertEquals(states, result);
    }
}