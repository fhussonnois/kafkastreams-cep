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
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.pattern.Selected;
import com.github.fhuss.kafka.streams.cep.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStore;
import com.github.fhuss.kafka.streams.cep.state.internal.AggregatesStoreImpl;
import com.github.fhuss.kafka.streams.cep.state.internal.SharedVersionedBufferStoreImpl;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.state.AggregatesStore;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;

public class NFATest {

    private Event<String, String> ev1 = new Event<>("ev1", "A", System.currentTimeMillis(), "test", 0, 0);
    private Event<String, String> ev2 = new Event<>("ev2", "B", System.currentTimeMillis(), "test", 0, 1);
    private Event<String, String> ev3 = new Event<>("ev3", "C", System.currentTimeMillis(), "test", 0, 2);
    private Event<String, String> ev4 = new Event<>("ev4", "C", System.currentTimeMillis(), "test", 0, 3);
    private Event<String, String> ev5 = new Event<>("ev5", "D", System.currentTimeMillis(), "test", 0, 4);
    private Event<String, String> ev6 = new Event<>("ev6", "D", System.currentTimeMillis(), "test", 0, 5);

    /**
     * Pattern : (A;B;C) / Events : A1, B2, C3
     *
     * R1: A1, B2, C3 (matched)
     * R2: _
     */
    @Test
    public void testNFAGivenOneRunAndStrictContiguity() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where((event, store) -> event.value().equals("A"))
                .then()
                .select("second")
                    .where((event, store) -> event.value().equals("B"))
                .then()
                .select("latest")
                    .where((event, store) -> event.value().equals("C"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected =  Sequence.<String, String>newBuilder()
                .add("latest", ev3)
                .add("second", ev2)
                .add("first", ev1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;B;C;D) / Events : A1, B2, C3, C4, D5
     *
     * R1: A1, B2, C3, C4, D5 (matched)
     * R2: _
     */
    @Test
    public void testNFAGivenOneRunAndMultipleMatch() {
        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("firstStage")
                    .where((event, store) -> event.value().equals("A"))
                    .then()
                .select("secondStage")
                    .where((event, store) -> event.value().equals("B"))
                    .then()
                .select("thirdStage")
                    .oneOrMore()
                    .where((event, store) -> event.value().equals("C"))
                    .then()
                .select("latestState")
                    .where((event, store) -> event.value().equals("D"))
                    .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected = Sequence.<String, String>newBuilder()
                .add("firstStage", ev1)
                .add("secondStage", ev2)
                .add("thirdStage", ev3)
                .add("thirdStage", ev4)
                .add("latestState", ev5)
                .build(false);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;B;C;D) / Events : A1, B2, C3, C4, D5
     *
     * R1: A1, B2, C3, C4, D5 (matched)
     * R2: _
     */
    @Test
    public void testNFAGivenTwoConsecutiveSkipTillNextMatch() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where((event, store) -> event.value().equals("A"))
                .then()
                .select("second", Selected.withSkipTilNextMatch())
                    .where((event, store) -> event.value().equals("C"))
                .then()
                .select("latest", Selected.withSkipTilNextMatch())
                    .where((event, store) -> event.value().equals("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);
        assertEquals(1, s.size());
        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev3)
                .add("latest", ev5)
                .build(false);
        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;C;D) / Events : A1, B2, C3, C4, D5
     *
     * R1: A1, B2, C3, C4, D5 (matched)
     * R2: _
     */
    @Test
    public void testNFAGivenTwoConsecutiveSkipTillNextMatchAndMultipleMatch() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where((event, store) -> event.value().equals("A"))
                .then()
                .select("second", Selected.withSkipTilNextMatch())
                    .oneOrMore()
                    .where((event, store) -> event.value().equals("C"))
                .then()
                .select("latest", Selected.withSkipTilNextMatch())
                    .where((event, store) -> event.value().equals("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);
        assertEquals(1, s.size());
        Sequence<String, String> expected = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev3)
                .add("second", ev4)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;C;D) / Events : A1, B2, C3, C4, D5
     *
     * R1: A1, C3, C4, D5 (matched)
     * R2: _
     * R3: A1, C4, D5 (matched)
     * R4: A1, _, _ , _
     * R5: A1, C3, D5 (matched)
     */
    @Test
    public void testNFAGivenMultipleMatchAndSkipTillAnyMatch() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                .where((event, store) -> event.value().equals("A"))
                .then()
                .select("second", Selected.withSkipTilAnyMatch())
                .oneOrMore()
                .where((event, store) -> event.value().equals("C"))
                .then()
                .select("latest")
                .where((event, store) -> event.value().equals("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);

        assertNFA(nfa, 5, 2);

        assertEquals(3, s.size());
        Sequence<String, String> expected = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev3)
                .add("second", ev4)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected, s.get(0));

        Sequence<String, String> expected2 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev3)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected2, s.get(1));

        Sequence<String, String> expected3 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev4)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected3, s.get(2));
    }

    /**
     * Pattern : (A;B;C;D) / Events : A1, B2, C3, C4, D5
     *
     * R1: A1, B2, C3, D5 (matched)
     * R2: _
     * R3: A1, B2, C4, D5 (matched)
     * R4: A1, B2, _ , _
     * R5: A1, B2, C3, _
     * R6: A1, B2, C4, _
     */
    @Test
    public void testNFAGivenTwoConsecutiveSkipTilAnyMatch() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                .where((event, store) -> event.value().equals("A"))
                .then()
                .select("second")
                .where((event, store) -> event.value().equals("B"))
                .then()
                .select("three", Selected.withSkipTilAnyMatch())
                .where((event, store) -> event.value().equals("C"))
                .then()
                .select("latest", Selected.withSkipTilAnyMatch())
                .where((event, store) -> event.value().equals("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);

        assertNFA(nfa, 6, 4);

        assertEquals(2, s.size());
        Sequence<String, String> expected1 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev2)
                .add("three", ev3)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected1, s.get(0));
        Sequence<String, String> expected2 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev2)
                .add("three", ev4)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected2, s.get(1));
    }
    /**
     * Pattern : (A;B;C;D) / Events : A1, B2, C3, C4, D5
     * R1: A1, B2, C3, D5 (matched)
     * R2: _
     * R3: A1, B2, C4, D5 (matched)
     * R4: A1, B2, _, _
     */
    @Test
    public void testNFAGivenMultipleStrategies() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where((event, store) -> event.value().equals("A"))
                .then()
                .select("second")
                    .where((event, store) -> event.value().equals("B"))
                .then()
                .select("three", Selected.withSkipTilAnyMatch())
                    .where((event, store) -> event.value().equals("C"))
                .then()
                .select("latest", Selected.withSkipTilNextMatch())
                    .where((event, store) -> event.value().equals("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);

        assertNFA(nfa, 4, 2);

        assertEquals(2, s.size());
        Sequence<String, String> expected1 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev2)
                .add("three", ev3)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected1, s.get(0));
        Sequence<String, String> expected2 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev2)
                .add("three", ev4)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected2, s.get(1));
    }

    /**
     * Pattern : (A;B;C;D) / Events : A1, B2, C3, D4, D5
     * R1: A1, B2, C3, D4
     * R2: _
     * R3: A1, B2, C3, D5
     * R4: A1, B2, C3, _
     */
    @Test
    public void testNFAGivenSkipTillAnyMatchOnLatestStage() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                .where((event, store) -> event.value().equals("A"))
                .then()
                .select("second")
                .where((event, store) -> event.value().equals("B"))
                .then()
                .select("three")
                .where((event, store) -> event.value().equals("C"))
                .then()
                .select("latest", Selected.withSkipTilAnyMatch())
                .where((event, store) -> event.value().equals("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());
        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev5, ev6);

        Assert.assertEquals(4, nfa.getRuns());


        Queue<ComputationStage<String, String>> stages = nfa.getComputationStages();
        Assert.assertEquals(2, stages.size());
        ComputationStage<String, String> stage1 = stages.poll();

        ComputationStage<String, String> stage2 = stages.poll();
        Assert.assertEquals(ev3, stage1.getLastEvent());
        Assert.assertEquals(4, stage1.getSequence());
        Assert.assertEquals("three", stage1.getStage().getName());

        Assert.assertEquals(null, stage2.getLastEvent());
        Assert.assertEquals(2, stage2.getSequence());
        Assert.assertEquals("first", stage2.getStage().getName());

        assertEquals(2, s.size());
        Sequence<String, String> expected1 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev2)
                .add("three", ev3)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected1, s.get(0));
        Sequence<String, String> expected2 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev2)
                .add("three", ev3)
                .add("latest", ev6)
                .build(false);

        assertEquals(expected2, s.get(1));
    }

    private void assertNFA(NFA<String, String> nfa, int runs, int stage) {
        Assert.assertEquals(runs, nfa.getRuns());
        Queue<ComputationStage<String, String>> computationStages = nfa.getComputationStages();
        Assert.assertEquals(stage, computationStages.size());
    }

    private <K, V> List<Sequence<K, V>> simulate(NFA<K, V> nfa, Event<K, V>...e) {
        List<Sequence<K, V>> s = new LinkedList<>();
        List<Event<K, V>> events = Arrays.asList(e);
        for(Event<K, V> event : events) {
            s.addAll(nfa.matchPattern(event));
        }
        return s;
    }

    private <K, V> NFA<K, V> newNFA(Pattern<K, V> pattern, Serde<K> keySerde, Serde<V> valSerde) {
        List<Stage<K, V>> stages = new StagesFactory<K, V>().make(pattern);

        SharedVersionedBufferStore<K, V> bufferStore =  new SharedVersionedBufferStoreImpl<>(
                new InMemoryKeyValueStore<>("test-buffer", Serdes.Bytes(), Serdes.ByteArray()), keySerde, valSerde);
        bufferStore.init(new NoOpProcessorContext(), null);

        AggregatesStore<K> aggStore = new AggregatesStoreImpl<>(
                new InMemoryKeyValueStore<>("test-aggregate", Serdes.Bytes(), Serdes.ByteArray())
        );

        aggStore.init(new NoOpProcessorContext(), null);

        return new NFA<>(aggStore, bufferStore, stages);
    }
}