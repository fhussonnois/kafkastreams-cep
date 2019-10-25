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
package com.github.fhuss.kafka.streams.cep.core.nfa;

import com.github.fhuss.kafka.streams.cep.core.Event;
import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.TestMatcher;
import com.github.fhuss.kafka.streams.cep.core.pattern.Selected;
import com.github.fhuss.kafka.streams.cep.core.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.core.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.core.state.AggregatesStore;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Aggregated;
import com.github.fhuss.kafka.streams.cep.core.state.internal.DelegateSharedVersionedBufferStore;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Matched;
import com.github.fhuss.kafka.streams.cep.core.state.internal.MatchedEvent;
import com.github.fhuss.kafka.streams.cep.core.state.internal.MatchedEventStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

public class NFATest {

    private Event<String, String> ev1 = new Event<>("ev1", "A", System.currentTimeMillis(), "test", 0, 0);
    private Event<String, String> ev2 = new Event<>("ev2", "B", System.currentTimeMillis(), "test", 0, 1);
    private Event<String, String> ev3 = new Event<>("ev3", "C", System.currentTimeMillis(), "test", 0, 2);
    private Event<String, String> ev4 = new Event<>("ev4", "C", System.currentTimeMillis(), "test", 0, 3);
    private Event<String, String> ev5 = new Event<>("ev5", "D", System.currentTimeMillis(), "test", 0, 4);
    private Event<String, String> ev6 = new Event<>("ev6", "C", System.currentTimeMillis(), "test", 0, 5);
    private Event<String, String> ev7 = new Event<>("ev7", "D", System.currentTimeMillis(), "test", 0, 6);
    private Event<String, String> ev8 = new Event<>("ev8", "E", System.currentTimeMillis(), "test", 0, 7);

    private AtomicInteger offset;

    @Before
    public void before() {
        this.offset = new AtomicInteger();
    }

    @Test
    public void testNFAGivenStatefulCondition() {
        Pattern<String, Integer> pattern = new QueryBuilder<String, Integer>()
                .select("first")
                    .where(TestMatcher.isGreaterThan(0))
                    .fold("sum", (key, value, state) -> value)
                    .fold("count", (key, value, state) -> 1)
                .then()
                    .select("second")
                    .oneOrMore()
                    .where((event, states) -> {
                        double average =(int)states.get("sum") / (int)states.get("count");
                        return average >= event.value();
                    })
                    .<Integer>fold("sum", (key, value, state) -> state + value)
                    .<Integer>fold("count", (key, value, state) -> state + 1)
                .then()
                    .select("latest")
                    .where((event, states) -> {
                        double average = (int)states.get("sum") / (int)states.get("count");
                        return average < event.value();
                    })
                .build();

        final NFA<String, Integer> nfa = newNFA(pattern);

        Event<String, Integer> e1 = nextEvent("t1", "key", 5);
        Event<String, Integer> e2 = nextEvent("t1", "key", 3);
        Event<String, Integer> e3 = nextEvent("t1", "key", 4);
        Event<String, Integer> e4 = nextEvent("t1", "key", 10);
        List<Sequence<String, Integer>> s = simulate(nfa, e1, e2, e3, e4);

        assertEquals(1, s.size());

        assertNFA(nfa, 5, 2);

        Sequence<String, Integer> expected =  Sequence.<String, Integer>newBuilder()
                .add("latest", e4)
                .add("second", e3)
                .add("second", e2)
                .add("first", e1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    @Test
    public void testNFAGivenSequenceCondition() {
        Pattern<String, Integer> pattern = new QueryBuilder<String, Integer>()
                .select("first")
                    .where(TestMatcher.isGreaterThan(0))
                    .then()
                .select("second")
                    .oneOrMore()
                    .where((event, sequence, states) ->  {
                        double average = StreamSupport.stream(sequence.spliterator(), false)
                                .mapToInt(Event::value)
                                .summaryStatistics()
                                .getAverage();
                        return average >= event.value();
                    })
                    .then()
                .select("latest")
                    .where((event, sequence, states) ->  {
                        double average = StreamSupport.stream(sequence.spliterator(), false)
                                .mapToInt(Event::value)
                                .summaryStatistics()
                                .getAverage();
                        return average < event.value();
                    })
                .build();

        final NFA<String, Integer> nfa = newNFA(pattern);

        Event<String, Integer> e1 = nextEvent("t1", "key", 5);
        Event<String, Integer> e2 = nextEvent("t1", "key", 3);
        Event<String, Integer> e3 = nextEvent("t1", "key", 4);
        Event<String, Integer> e4 = nextEvent("t1", "key", 10);
        List<Sequence<String, Integer>> s = simulate(nfa, e1, e2, e3, e4);

        assertEquals(1, s.size());

        assertNFA(nfa, 5, 2);

        Sequence<String, Integer> expected =  Sequence.<String, Integer>newBuilder()
                .add("latest", e4)
                .add("second", e3)
                .add("second", e2)
                .add("first", e1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;C{3} E) / Events : A1, C3, C4, C6, E8
     *
     * R1: A1, C3, C4, E8(matched)
     * R2: _
     */
    @Test
    public void testNFAGivenExpectingOccurrencesStage() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where(TestMatcher.isEqualTo("A"))
                    .then()
                .select("second")
                    .times(3)
                    .where(TestMatcher.isEqualTo("C"))
                .then()
                    .select("latest")
                    .where(TestMatcher.isEqualTo("E"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev3, ev4, ev6, ev8);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected =  Sequence.<String, String>newBuilder()
                .add("latest", ev8)
                .add("second", ev6)
                .add("second", ev4)
                .add("second", ev3)
                .add("first", ev1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;C*; E) / Events : A1, D5
     *
     * R1: A1, D5(matched)
     * R2: _
     */
    @Test
    public void testNFAGivenZeroOrMoreStageWhenNoMatchingInputs() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where(TestMatcher.isEqualTo("A"))
                    .then()
                .select("second")
                    .zeroOrMore()
                    .where(TestMatcher.isEqualTo("C"))
                    .then()
                .select("latest")
                    .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev5);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected =  Sequence.<String, String>newBuilder()
                .add("latest", ev5)
                .add("first", ev1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;C*; E) / Events : A1, C3, C4, D5
     *
     * R1: A1, C3, C4, D5(matched)
     * R2: _
     */
    @Test
    public void testNFAGivenZeroOrMoreStageWhenMatchingInputs() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where(TestMatcher.isEqualTo("A"))
                    .then()
                .select("second")
                    .zeroOrMore()
                    .where(TestMatcher.isEqualTo("C"))
                    .then()
                .select("latest")
                    .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev3, ev4, ev5);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected =  Sequence.<String, String>newBuilder()
                .add("latest", ev5)
                .add("second", ev4)
                .add("second", ev3)
                .add("first", ev1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;C{2}?; E) / Events : A1, D5
     *
     * R1: A1, D5(matched)
     * R2: _
     */
    @Test
    public void testNFAGivenOptionalExpectingOccurrenceStageWhenNoMatchingInputs() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where(TestMatcher.isEqualTo("A"))
                    .then()
                .select("second")
                    .times(2)
                    .optional()
                    .where(TestMatcher.isEqualTo("C"))
                .then()
                    .select("latest")
                    .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev5);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected =  Sequence.<String, String>newBuilder()
                .add("latest", ev5)
                .add("first", ev1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;C{2}?; E) / Events : A1, C3, C4, D5
     *
     * R1: A1, C3, C4, D5(matched)
     * R2: _
     */
    @Test
    public void testNFAGivenOptionalExpectingOccurrenceStageWhenMatchingInputs() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second")
                .times(2)
                .optional()
                .where(TestMatcher.isEqualTo("C"))
                .then()
                .select("latest")
                .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev3, ev4, ev5);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected =  Sequence.<String, String>newBuilder()
                .add("latest", ev5)
                .add("second", ev4)
                .add("second", ev3)
                .add("first", ev1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;C{3} E) / Events : A1, C3, C4, D5, C6, E8
     *
     * R1: A1, C3, C4, C6, E7 (matched)
     * R2: _
     */
    @Test
    public void testNFAGivenExpectingOccurrencesStageWhenSkipTilNextMatchContiguityIsUsed() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where(TestMatcher.isEqualTo("A"))
                    .then()
                .select("second", Selected.withSkipTilNextMatch())
                    .times(3)
                    .where(TestMatcher.isEqualTo("C"))
                    .then()
                .select("latest")
                    .where(TestMatcher.isEqualTo("E"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev3, ev4, ev5, ev6, ev8);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected =  Sequence.<String, String>newBuilder()
                .add("latest", ev8)
                .add("second", ev6)
                .add("second", ev4)
                .add("second", ev3)
                .add("first", ev1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

    /**
     * Pattern : (A;B;C) / Events : A1, C3
     *
     * R1: A1, C3(matched)
     * R2: _
     */
    @Test
    public void testNFAGivenOptionalStageAndStrictContiguity() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where(TestMatcher.isEqualTo("A"))
                .then()
                    .select("second")
                    .optional()
                    .where(TestMatcher.isEqualTo("B"))
                .then()
                    .select("latest")
                    .where(TestMatcher.isEqualTo("C"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev3);
        assertEquals(1, s.size());

        assertNFA(nfa, 2, 1);

        Sequence<String, String> expected =  Sequence.<String, String>newBuilder()
                .add("latest", ev3)
                .add("first", ev1)
                .build(true);

        assertEquals(expected, s.get(0));
    }

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
                    .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second")
                    .where(TestMatcher.isEqualTo("B"))
                .then()
                .select("latest")
                    .where(TestMatcher.isEqualTo("C"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

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
                    .where(TestMatcher.isEqualTo("A"))
                    .then()
                .select("secondStage")
                    .where(TestMatcher.isEqualTo("B"))
                    .then()
                .select("thirdStage")
                    .oneOrMore()
                    .where(TestMatcher.isEqualTo("C"))
                    .then()
                .select("latestState")
                    .where(TestMatcher.isEqualTo("D"))
                    .build();

        final NFA<String, String> nfa = newNFA(pattern);

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
                    .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second", Selected.withSkipTilNextMatch())
                    .where(TestMatcher.isEqualTo("C"))
                .then()
                .select("latest", Selected.withSkipTilNextMatch())
                    .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

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
                    .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second", Selected.withSkipTilNextMatch())
                    .oneOrMore()
                    .where(TestMatcher.isEqualTo("C"))
                .then()
                .select("latest", Selected.withSkipTilNextMatch())
                    .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

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
     * R1: A1, C3, D5 (matched)
     * R2: _
     * R3: A1, C4, D5 (matched)
     * R4: A1, _, _ , _
     * R5: A1, C3, _
     * R6: A1, C4, _
     */
    @Test
    public void testNFAGivenTwoConsecutiveSkipTillAnyMatch() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second", Selected.withSkipTilAnyMatch())
                .where(TestMatcher.isEqualTo("C"))
                .then()
                .select("latest", Selected.withSkipTilAnyMatch())
                .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);

        assertNFA(nfa, 6, 4);

        assertEquals(2, s.size());
        Sequence<String, String> expected = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev3)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected, s.get(0));

        Sequence<String, String> expected2 = Sequence.<String, String>newBuilder()
                .add("first", ev1)
                .add("second", ev4)
                .add("latest", ev5)
                .build(false);

        assertEquals(expected2, s.get(1));
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
                .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second", Selected.withSkipTilAnyMatch())
                .oneOrMore()
                .where(TestMatcher.isEqualTo("C"))
                .then()
                .select("latest")
                .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

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
                .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second")
                .where(TestMatcher.isEqualTo("B"))
                .then()
                .select("three", Selected.withSkipTilAnyMatch())
                .where(TestMatcher.isEqualTo("C"))
                .then()
                .select("latest", Selected.withSkipTilAnyMatch())
                .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

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
                    .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second")
                    .where(TestMatcher.isEqualTo("B"))
                .then()
                .select("three", Selected.withSkipTilAnyMatch())
                    .where(TestMatcher.isEqualTo("C"))
                .then()
                .select("latest", Selected.withSkipTilNextMatch())
                    .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);

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
                .where(TestMatcher.isEqualTo("A"))
                .then()
                .select("second")
                .where(TestMatcher.isEqualTo("B"))
                .then()
                .select("three")
                .where(TestMatcher.isEqualTo("C"))
                .then()
                .select("latest", Selected.withSkipTilAnyMatch())
                .where(TestMatcher.isEqualTo("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern);
        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev5, ev7);

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
                .add("latest", ev7)
                .build(false);

        assertEquals(expected2, s.get(1));
    }

    private <K, V> void assertNFA(NFA<K, V> nfa, int runs, int stage) {
        Assert.assertEquals(runs, nfa.getRuns());
        Queue<ComputationStage<K, V>> computationStages = nfa.getComputationStages();
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

    private <K, V> NFA<K, V> newNFA(final Pattern<K, V> pattern) {
        StagesFactory<K, V> factory = new StagesFactory<>();
        Stages<K, V> stages = factory.make(pattern);

        final DelegateSharedVersionedBufferStore<K, V> bufferStore = new DelegateSharedVersionedBufferStore<>(
            new InMemoryMatchedEventStore<>()
        );

        AggregatesStore<K> aggregatesStore = new InMemoryAggregatesStore<>();

        return NFA.build(stages, aggregatesStore, bufferStore);
    }

    private <K, V> Event<K, V> nextEvent(String topic, int partition, K key, V value) {
        return new Event<>(key, value, System.currentTimeMillis(), topic, partition, offset.getAndIncrement());
    }

    private <K, V> Event<K, V> nextEvent(String topic, K key, V value) {
        return nextEvent(topic, 0, key, value);
    }


    private static final class InMemoryAggregatesStore<K> implements AggregatesStore<K> {

        private Map<Aggregated<K>, Object> map = new HashMap<>();

        @Override
        public <T> T find(final Aggregated<K> aggregated) {
            return (T) map.get(aggregated);
        }

        @Override
        public <T> void put(final Aggregated<K> aggregated, T aggregate) {
            map.put(aggregated, aggregate);
        }
    }

    private static final class InMemoryMatchedEventStore<K, V> implements MatchedEventStore<K, V> {

        private Map<Matched, MatchedEvent<K, V>> map = new HashMap<>();

        @Override
        public MatchedEvent<K, V> get(final Matched matched) {
            return map.get(matched);
        }

        @Override
        public void put(final Matched matched, final MatchedEvent<K, V> event) {
            map.put(matched, event);
        }

        @Override
        public void delete(final Matched matched) {
            map.remove(matched);
        }
    }

}