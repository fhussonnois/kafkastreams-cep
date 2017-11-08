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
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.KVSharedVersionedBuffer;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.TimedKeyValue;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.StackEventKey;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.TimedKeyValueSerDes;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.serde.KryoSerDe;
import com.github.fhuss.kafka.streams.cep.state.StateStoreProvider;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class NFATest {

    public static final String PATTERN_TEST = "test";
    private Event<String, String> ev1 = new Event<>(null, "A", System.currentTimeMillis(), "test", 0, 0);
    private Event<String, String> ev2 = new Event<>(null, "B", System.currentTimeMillis(), "test", 0, 1);
    private Event<String, String> ev3 = new Event<>(null, "C", System.currentTimeMillis(), "test", 0, 2);
    private Event<String, String> ev4 = new Event<>(null, "C", System.currentTimeMillis(), "test", 0, 3);
    private Event<String, String> ev5 = new Event<>(null, "D", System.currentTimeMillis(), "test", 0, 4);

    @Test
    public void testNFAWithOneRunAndStrictContiguity() {

        Pattern<String, String> query = new QueryBuilder<String, String>()
                .select("first")
                    .where((key, value, timestamp, store) -> value.equals("A"))
                .then()
                .select("second")
                    .where((key, value, timestamp, store) -> value.equals("B"))
                .then()
                .select("latest")
                    .where((key, value, timestamp, store) -> value.equals("C"))
                .build();

        List<Stage<String, String>> stages = new StagesFactory<String, String>().make(query);
        DummyProcessorContext context = new DummyProcessorContext();
        NFA<String, String> nfa = new NFA<>(new StateStoreProvider("test", context), getInMemorySharedBuffer(Serdes.String(), Serdes.String()), stages);

        List<Sequence<String, String>> s = simulate(nfa, context, ev1, ev2, ev3);
        assertEquals(1, s.size());
        Sequence<String, String> expected = new Sequence<String, String>()
                .add("first", ev1)
                .add("second", ev2)
                .add("latest", ev3);

        assertEquals(expected, s.get(0));
    }

    @Test
    public void testNFAWithOneRunAndMultipleMatch() {
        Pattern<String, String> query = new QueryBuilder<String, String>()
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

        List<Stage<String, String>> stages = new StagesFactory<String, String>().make(query);
        DummyProcessorContext context = new DummyProcessorContext();
        NFA<String, String> nfa = new NFA<>(new StateStoreProvider("test", context), getInMemorySharedBuffer(Serdes.String(), Serdes.String()), stages);

        List<Sequence<String, String>> s = simulate(nfa, context, ev1, ev2, ev3, ev4, ev5);
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

        List<Stage<String, String>> stages = new StagesFactory<String, String>().make(pattern);
        DummyProcessorContext context = new DummyProcessorContext();
        NFA<String, String> nfa = new NFA<>(new StateStoreProvider("test", context), getInMemorySharedBuffer(Serdes.String(), Serdes.String()), stages);

        List<Sequence<String, String>> s = simulate(nfa, context, ev1, ev2, ev3, ev4, ev5);
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

        List<Stage<String, String>> stages = new StagesFactory<String, String>().make(pattern);
        DummyProcessorContext context = new DummyProcessorContext();
        NFA<String, String> nfa = new NFA<>(new StateStoreProvider("test", context), getInMemorySharedBuffer(Serdes.String(), Serdes.String()), stages);

        List<Sequence<String, String>> s = simulate(nfa, context, ev1, ev2, ev3, ev4, ev5);
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

    private <K, V> List<Sequence<K, V>> simulate(NFA<K, V> nfa, DummyProcessorContext context, Event<K, V>...e) {
        List<Sequence<K, V>> s = new LinkedList<>();
        List<Event<K, V>> events = Arrays.asList(e);
        for(Event<K, V> event : events) {
            context.set(event.topic, event.partition, event.offset);
            s.addAll(nfa.matchPattern(event));
        }
        return s;
    }

    @SuppressWarnings("unchecked")
    private <K, V> KVSharedVersionedBuffer<K, V> getInMemorySharedBuffer(Serde<K> keySerDe, Serde<V> valueSerDe) {
        return new KVSharedVersionedBuffer<>(newMemoryKeyValueStore("test", keySerDe, valueSerDe));
    }

    @SuppressWarnings("unchecked")
    private <K, V> InMemoryKeyValueStore<StackEventKey, TimedKeyValue<K, V>> newMemoryKeyValueStore(
            final String name,
            final Serde<K> keySerDe,
            final Serde<V> valueSerDe) {
        TimedKeyValueSerDes<K, V> timedKeyValueSerDes = new TimedKeyValueSerDes<>(keySerDe, valueSerDe);
        KryoSerDe<StackEventKey> kryoSerDe = new KryoSerDe<>();
        return new InMemoryKeyValueStore<>(name, kryoSerDe, Serdes.serdeFrom(timedKeyValueSerDes, timedKeyValueSerDes));
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

        StockEvent e1 = new StockEvent(100, 1010);
        StockEvent e2 = new StockEvent(120, 990);
        StockEvent e3 = new StockEvent(120, 1005);
        StockEvent e4 = new StockEvent(121, 999);
        StockEvent e5 = new StockEvent(120, 999);
        StockEvent e6 = new StockEvent(125, 750);
        StockEvent e7 = new StockEvent(120, 950);
        StockEvent e8 = new StockEvent(120, 700);

        Pattern<Object, StockEvent> pattern = new QueryBuilder<Object, StockEvent>()
                .select()
                    .where((k, v, ts, store) -> v.volume > 1000)
                    .<Integer>fold("avg", (k, v, curr) -> v.price)
                    .then()
                .select()
                    .zeroOrMore()
                    .skipTillNextMatch()
                .where((k, v, ts, state) -> v.price > (int)state.get("avg"))
                .<Integer>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                .<Integer>fold("volume", (k, v, curr) -> v.volume)
                .then()
                .select()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.volume < 0.8 * state.getOrElse("volume", 0))
                    .within(1, TimeUnit.HOURS)
                .build();

        List<Stage<Object, StockEvent>> stages = new StagesFactory<Object, StockEvent>().make(pattern);
        DummyProcessorContext context = new DummyProcessorContext();
        context.register(newMemoryKeyValueStore(StateStoreProvider.getStateStoreName(PATTERN_TEST, "avg" ), Serdes.Long(), Serdes.Long()), false, null);
        context.register(newMemoryKeyValueStore(StateStoreProvider.getStateStoreName(PATTERN_TEST, "volume"), Serdes.Long(), Serdes.Long()), false, null);
        KVSharedVersionedBuffer<Object, StockEvent> inMemorySharedBuffer = getInMemorySharedBuffer(new KryoSerDe<>(), new KryoSerDe<>());
        NFA<Object, StockEvent> nfa = new NFA<>(new StateStoreProvider(PATTERN_TEST, context), inMemorySharedBuffer, stages);

        AtomicLong offset = new AtomicLong(0);
        List<Event<Object, StockEvent>> collect = Arrays.asList(new StockEvent[]{e1, e2, e3, e4, e5, e6, e7, e8})
                .stream().map(e -> new Event<>(null, e, System.currentTimeMillis(), "test", 0, offset.getAndIncrement()))
                .collect(Collectors.toList());
        List<Sequence<Object, StockEvent>> s = simulate(nfa, context, collect.toArray(new Event[collect.size()]));
        assertEquals(4, s.size());
    }

    public static class StockEvent {
        public final int price;
        public final int volume;

        public StockEvent(int price, int volume) {
            this.price = price;
            this.volume = volume;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("StockEvent{");
            sb.append("price=").append(price);
            sb.append(", volume=").append(volume);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class DummyProcessorContext implements ProcessorContext {

        public int partition;
        public long offset;
        public String topic;

        public Map<String, StateStore> stores = new HashMap<>();

        public void set(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public String applicationId() {
            return null;
        }

        @Override
        public TaskId taskId() {
            return null;
        }

        @Override
        public Serde<?> keySerde() {
            return null;
        }

        @Override
        public Serde<?> valueSerde() {
            return null;
        }

        @Override
        public File stateDir() {
            return null;
        }

        @Override
        public StreamsMetrics metrics() {
            return null;
        }

        @Override
        public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
            this.stores.put(store.name(), store);
        }

        @Override
        public StateStore getStateStore(String name) {
            return stores.get(name);
        }

        @Override
        public Cancellable schedule(long interval, PunctuationType type, Punctuator callback) {
            return null;
        }

        @Override
        public void schedule(long interval) {

        }

        @Override
        public <K, V> void forward(K key, V value) {

        }

        @Override
        public <K, V> void forward(K key, V value, int childIndex) {

        }

        @Override
        public <K, V> void forward(K key, V value, String childName) {

        }

        @Override
        public void commit() {

        }

        @Override
        public String topic() {
            return topic;
        }

        @Override
        public int partition() {
            return partition;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public long timestamp() {
            return System.currentTimeMillis();
        }

        @Override
        public Map<String, Object> appConfigs() {
            return null;
        }

        @Override
        public Map<String, Object> appConfigsWithPrefix(String prefix) {
            return null;
        }
    }

}