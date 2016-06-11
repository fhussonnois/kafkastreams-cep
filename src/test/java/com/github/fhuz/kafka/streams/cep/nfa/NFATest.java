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
import java.util.List;

import static org.junit.Assert.*;

public class NFATest {

    @Test
    public void testNFAWithOneRunAndStrictContiguity() {
        List<State<String, String>> states = new NFAFactory<String, String>().make(getStrictContiguityPattern());
        DummyProcessorContext context = new DummyProcessorContext();
        NFA<String, String> nfa = new NFA<>(context, getInMemorySharedBuffer(), states);

        List<Sequence<String, String>> s;

        Event<String, String> ev1 = new Event<>(null, "A", System.currentTimeMillis(), "test", 0, 0);
        Event<String, String> ev2 = new Event<>(null, "B", System.currentTimeMillis(), "test", 0, 1);
        Event<String, String> ev3 = new Event<>(null, "C", System.currentTimeMillis(), "test", 0, 2);

        context.set("test", 0, 0L);
        s = nfa.matchPattern(null, ev1.value, ev1.timestamp);
        assertTrue(s.isEmpty());
        context.set("test", 0, 1L);
        s = nfa.matchPattern(null, ev2.value, ev2.timestamp);
        assertTrue(s.isEmpty());
        context.set("test", 0, 2L);
        s = nfa.matchPattern(null, ev3.value, ev3.timestamp);
        assertEquals(1, s.size());
        Sequence<String, String> seq = s.get(0);

        assertEquals(3, seq.size());
        assertNotNull(seq.get("latest").get(0).equals(ev3));
        assertNotNull(seq.get("second").get(0).equals(ev2));
        assertNotNull(seq.get("first").get(0).equals(ev1));
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