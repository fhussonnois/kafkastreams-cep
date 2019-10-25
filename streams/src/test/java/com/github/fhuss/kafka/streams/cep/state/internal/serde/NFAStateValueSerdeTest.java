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