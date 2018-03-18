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
import com.github.fhuss.kafka.streams.cep.demo.StockEvent;
import com.github.fhuss.kafka.streams.cep.demo.StockEventSerde;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStore;
import com.github.fhuss.kafka.streams.cep.state.internal.AggregatesStoreImpl;
import com.github.fhuss.kafka.streams.cep.state.internal.SharedVersionedBufferStoreImpl;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.state.AggregatesStore;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class NFATest {

    private Event<String, String> ev1 = new Event<>(null, "A", System.currentTimeMillis(), "test", 0, 0);
    private Event<String, String> ev2 = new Event<>(null, "B", System.currentTimeMillis(), "test", 0, 1);
    private Event<String, String> ev3 = new Event<>(null, "C", System.currentTimeMillis(), "test", 0, 2);
    private Event<String, String> ev4 = new Event<>(null, "C", System.currentTimeMillis(), "test", 0, 3);
    private Event<String, String> ev5 = new Event<>(null, "D", System.currentTimeMillis(), "test", 0, 4);

    @Test
    public void testNFAWithOneRunAndStrictContiguity() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where((key, value, timestamp, store) -> value.equals("A"))
                .then()
                .select("second")
                    .where((key, value, timestamp, store) -> value.equals("B"))
                .then()
                .select("latest")
                    .where((key, value, timestamp, store) -> value.equals("C"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3);
        assertEquals(1, s.size());
        Sequence<String, String> expected = new Sequence<String, String>()
                .add("first", ev1)
                .add("second", ev2)
                .add("latest", ev3);

        assertEquals(expected, s.get(0));
    }

    @Test
    public void testNFAWithOneRunAndMultipleMatch() {
        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("firstStage")
                    .where((key, value, timestamp, store) -> value.equals("A"))
                    .then()
                .select("secondStage")
                    .where((key, value, timestamp, store) -> value.equals("B"))
                    .then()
                .select("thirdStage")
                    .oneOrMore()
                    .where((key, value, timestamp, store) -> value.equals("C"))
                    .then()
                .select("latestState")
                    .where((key, value, timestamp, store) -> value.equals("D"))
                    .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);
        assertEquals(1, s.size());

        Sequence<String, String> expected = new Sequence<String, String>()
                .add("firstStage", ev1)
                .add("secondStage", ev2)
                .add("thirdStage", ev3)
                .add("thirdStage", ev4)
                .add("latestState", ev5);

        assertEquals(expected, s.get(0));
    }


    @Test
    public void testNFAWithSkipTillNextMatch() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where((key, value, timestamp, store) -> value.equals("A"))
                .then()
                .select("second")
                    .skipTillNextMatch()
                    .where((key, value, timestamp, store) -> value.equals("C"))
                .then()
                .select("latest")
                    .skipTillNextMatch()
                    .where((key, value, timestamp, store) -> value.equals("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);
        assertEquals(1, s.size());
        Sequence<String, String> expected = new Sequence<String, String>()
                .add("first", ev1)
                .add("second", ev3)
                .add("latest", ev5);

        assertEquals(expected, s.get(0));
    }

    @Test
    public void testNFAWithSkipTillAnyMatch() {

        Pattern<String, String> pattern = new QueryBuilder<String, String>()
                .select("first")
                    .where((key, value, timestamp, store) -> value.equals("A"))
                .then()
                .select("second")
                    .where((key, value, timestamp, store) -> value.equals("B"))
                .then()
                .select("three")
                    .skipTillAnyMatch()
                    .where((key, value, timestamp, store) -> value.equals("C"))
                .then()
                .select("latest")
                    .skipTillAnyMatch()
                    .where((key, value, timestamp, store) -> value.equals("D"))
                .build();

        final NFA<String, String> nfa = newNFA(pattern, Serdes.String(), Serdes.String());

        List<Sequence<String, String>> s = simulate(nfa, ev1, ev2, ev3, ev4, ev5);
        assertEquals(2, s.size());
        Sequence<String, String> expected1 = new Sequence<String, String>()
                .add("first", ev1)
                .add("second", ev2)
                .add("three", ev3)
                .add("latest", ev5);

        assertEquals(expected1, s.get(0));
        Sequence<String, String> expected2 = new Sequence<String, String>()
                .add("first", ev1)
                .add("second", ev2)
                .add("three", ev4)
                .add("latest", ev5);
        assertEquals(expected2, s.get(1));
    }

    private <K, V> List<Sequence<K, V>> simulate(NFA<K, V> nfa, Event<K, V>...e) {
        List<Sequence<K, V>> s = new LinkedList<>();
        List<Event<K, V>> events = Arrays.asList(e);
        for(Event<K, V> event : events) {
            s.addAll(nfa.matchPattern(event));
        }
        return s;
    }

    /**
     * PATTERN SEQ(Stock+ a[ ], Stock b)
     *  WHERE skip_till_next_match(a[ ], b) {
     *      [symbol]
     *  and
     *      a[1].volume > 1000
     *  and
     *      a[i].price > avg(a[..i-1].price)
     *  and
     *      b.volume < 80%*a[a.LEN].volume }
     *  WITHIN 1 hour
     */
    @Test
    public void testComplexPatternWithState() {

        StockEvent e1 = new StockEvent("e1", 100, 1010);
        StockEvent e2 = new StockEvent("e2", 120, 990);
        StockEvent e3 = new StockEvent("e3", 120, 1005);
        StockEvent e4 = new StockEvent("e4", 121, 999);
        StockEvent e5 = new StockEvent("e5", 120, 999);
        StockEvent e6 = new StockEvent("e6", 125, 750);
        StockEvent e7 = new StockEvent("e7", 120, 950);
        StockEvent e8 = new StockEvent("e8", 120, 700);

        Pattern<String, StockEvent> pattern = new QueryBuilder<String, StockEvent>()
                .select()
                    .where((k, v, ts, store) -> v.volume > 1000)
                    .<Long>fold("avg", (k, v, curr) -> v.price)
                    .then()
                .select()
                    .zeroOrMore()
                    .skipTillNextMatch()
                .where((k, v, ts, state) -> v.price > (long) state.get("avg"))
                .<Long>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                .<Long>fold("volume", (k, v, curr) -> v.volume)
                .then()
                .select()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.volume < 0.8 * (long) state.getOrElse("volume", 0L))
                    .within(1, TimeUnit.HOURS)
                .build();

        final NFA<String, StockEvent> nfa = newNFA(pattern, Serdes.String(), new StockEventSerde());

        AtomicLong offset = new AtomicLong(0);
        List<Event<Object, StockEvent>> collect = Arrays.asList(new StockEvent[]{e1, e2, e3, e4, e5, e6, e7, e8})
                .stream()
                .map(e -> new Event<>(null, e, System.currentTimeMillis(), "test", 0, offset.getAndIncrement()))
                .collect(Collectors.toList());

        List<Sequence<String, StockEvent>> s = simulate(nfa, collect.toArray(new Event[collect.size()]));
        assertEquals(4, s.size());
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