package com.github.fhuz.kafka.streams.cep.nfa;

import com.github.fhuz.kafka.streams.cep.Event;
import com.github.fhuz.kafka.streams.cep.Sequence;
import com.github.fhuz.kafka.streams.cep.State;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.KVSharedVersionedBuffer;
import com.github.fhuz.kafka.streams.cep.pattern.EventSequence;
import com.github.fhuz.kafka.streams.cep.pattern.NFAFactory;
import com.github.fhuz.kafka.streams.cep.pattern.Pattern;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.MemoryLRUCache;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class NFATest {

    private Event<String, String> ev1 = new Event<>(null, "A", System.currentTimeMillis(), "test", 0, 0);
    private Event<String, String> ev2 = new Event<>(null, "B", System.currentTimeMillis(), "test", 0, 1);
    private Event<String, String> ev3 = new Event<>(null, "B", System.currentTimeMillis(), "test", 0, 2);
    private Event<String, String> ev4 = new Event<>(null, "C", System.currentTimeMillis(), "test", 0, 3);
    private Event<String, String> ev5 = new Event<>(null, "C", System.currentTimeMillis(), "test", 0, 4);

    @Test
    public void testNFAWithOneRunAndStrictContiguity() {
        List<State<String, String>> states = new NFAFactory<String, String>().make(getStrictContiguityPattern());
        DummyProcessorContext context = new DummyProcessorContext();
        NFA<String, String> nfa = new NFA<>(context, getInMemorySharedBuffer(), states);

        List<Sequence<String, String>> s = simulate(nfa, context, ev1, ev2, ev4);
        assertEquals(1, s.size());
        Sequence<String, String> expected = new Sequence<String, String>()
                .add("first", ev1)
                .add("second", ev2)
                .add("latest", ev4);

        assertEquals(expected, s.get(0));
    }

    @Test
    public void testNFAWithOneRunAndMultipleMatch() {
        List<State<String, String>> states = new NFAFactory<String, String>().make(getStrictContiguityPatternWithMultipleMatch());
        DummyProcessorContext context = new DummyProcessorContext();
        NFA<String, String> nfa = new NFA<>(context, getInMemorySharedBuffer(), states);

        List<Sequence<String, String>> s = simulate(nfa, context, ev1, ev2, ev3, ev4);
        assertEquals(1, s.size());

        Sequence<String, String> expected = new Sequence<String, String>()
                .add("first", ev1)
                .add("second", ev3)
                .add("second", ev2)
                .add("latest", ev4);

        assertEquals(expected, s.get(0));

    }

    private List<Sequence<String, String>> simulate(NFA<String, String> nfa, DummyProcessorContext context, Event<String, String>...e) {
        List<Sequence<String, String>> s = new LinkedList<>();
        List<Event<String, String>> events = Arrays.asList(e);
        for(Event<String, String> event : events) {
            assertTrue(s.isEmpty());
            context.set(event.topic, event.partition, event.offset);
            s = nfa.matchPattern(null, event.value, event.timestamp);
        }
        return s;
    }

    private Pattern<String, String> getStrictContiguityPattern() {
        return new EventSequence<String, String>()
                .select("first")
                .where((key, value, timestamp, store) -> value.equals("A"))
                .followBy("second")
                .where((key, value, timestamp, store) -> value.equals("B"))
                .followBy("latest")
                .where((key, value, timestamp, store) -> value.equals("C"));
    }

    private Pattern<String, String> getStrictContiguityPatternWithMultipleMatch() {
        return new EventSequence<String, String>()
                .select("first")
                .where((key, value, timestamp, store) -> value.equals("A"))
                .followBy("second")
                .where((key, value, timestamp, store) -> value.equals("B"))
                .oneOrMore()
                .followBy("latest")
                .where((key, value, timestamp, store) -> value.equals("C"));
    }

    @SuppressWarnings("unchecked")
    private <K, V> KVSharedVersionedBuffer<K, V> getInMemorySharedBuffer() {
        KeyValueStore<KVSharedVersionedBuffer.StackEventKey, KVSharedVersionedBuffer.TimedKeyValue> store = new MemoryLRUCache<>("test", 100);
        return new KVSharedVersionedBuffer<>(store);
    }

    public static class DummyProcessorContext implements ProcessorContext {

        public int partition;
        public long offset;
        public String topic;

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

        }

        @Override
        public StateStore getStateStore(String name) {
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
    }

}