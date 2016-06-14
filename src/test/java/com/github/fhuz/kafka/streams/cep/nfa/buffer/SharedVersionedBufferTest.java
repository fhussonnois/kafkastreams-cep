package com.github.fhuz.kafka.streams.cep.nfa.buffer;

import com.github.fhuz.kafka.streams.cep.Sequence;
import com.github.fhuz.kafka.streams.cep.nfa.Stage;
import com.github.fhuz.kafka.streams.cep.nfa.DeweyVersion;
import com.github.fhuz.kafka.streams.cep.Event;
import org.apache.kafka.streams.state.KeyValueStore;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.KVSharedVersionedBuffer.StackEventKey;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.KVSharedVersionedBuffer.TimedKeyValue;
import org.apache.kafka.streams.state.internals.MemoryLRUCache;
import org.junit.Test;

import static org.junit.Assert.*;

public class SharedVersionedBufferTest {

    private static Event<String, String> ev1 = new Event<>("k1", "v1", 1000000001L, "topic-test", 0, 0L);
    private static Event<String, String> ev2 = new Event<>("k2", "v2", 1000000002L, "topic-test", 0, 1L);
    private static Event<String, String> ev3 = new Event<>("k3", "v3", 1000000003L, "topic-test", 0, 2L);
    private static Event<String, String> ev4 = new Event<>("k4", "v4", 1000000004L, "topic-test", 0, 3L);
    private static Event<String, String> ev5 = new Event<>("k5", "v5", 1000000005L, "topic-test", 0, 4L);

    private static Stage<String, String> first  = new Stage<>("first", Stage.StateType.BEGIN);
    private static Stage<String, String> second = new Stage<>("second", Stage.StateType.NORMAL);
    private static Stage<String, String> latest = new Stage<>("latest", Stage.StateType.FINAL);

    @Test
    public void testExtractPatternsWithOneRun() {
        KVSharedVersionedBuffer<String, String> buffer = this.<String, String>getInMemorySharedBuffer();
        buffer.put(first, ev1, new DeweyVersion("1"));
        buffer.put(second, ev2, first, ev1, new DeweyVersion("1.0"));
        buffer.put(latest, ev3, second, ev2, new DeweyVersion("1.0.0"));

        Sequence<String, String> sequence = buffer.get(latest, ev3, new DeweyVersion("1.0.0"));
        assertNotNull(sequence);
        assertEquals(3, sequence.size());
        assertNotNull(sequence.get("latest").get(0).equals(ev3));
        assertNotNull(sequence.get("second").get(0).equals(ev2));
        assertNotNull(sequence.get("first").get(0).equals(ev1));
    }

    @Test
    public void testExtractPatternsWithBranchingRun() {
        KVSharedVersionedBuffer<String, String> buffer = this.<String, String>getInMemorySharedBuffer();

        buffer.put(first, ev1, new DeweyVersion("1"));
        buffer.put(second, ev2, first, ev1, new DeweyVersion("1.0"));
        buffer.put(latest, ev3, second, ev2, new DeweyVersion("1.0.0"));

        buffer.put(second, ev3, second, ev2,  new DeweyVersion("1.1"));
        buffer.put(second, ev4, second, ev3, new DeweyVersion("1.1"));
        buffer.put(latest, ev5, second, ev4, new DeweyVersion("1.1.0"));

        Sequence<String, String> sequence1 = buffer.get(latest, ev3, new DeweyVersion("1.0.0"));
        assertNotNull(sequence1);
        assertEquals(3, sequence1.size());
        assertNotNull(sequence1.get("latest").get(0).equals(ev3));
        assertNotNull(sequence1.get("second").get(0).equals(ev2));
        assertNotNull(sequence1.get("first").get(0).equals(ev1));

        Sequence<String, String> sequence2 = buffer.get(latest, ev5, new DeweyVersion("1.1.0"));
        assertNotNull(sequence2);
        assertEquals(5, sequence2.size());
        assertEquals(1, sequence2.get("latest").size());
        assertEquals(3, sequence2.get("second").size());
        assertEquals(1, sequence2.get("first").size());
    }

    @SuppressWarnings("unchecked")
    private <K, V> KVSharedVersionedBuffer<K, V> getInMemorySharedBuffer() {
        KeyValueStore<StackEventKey<K, V>, TimedKeyValue<K, V>> store = new MemoryLRUCache<>("test", 100);
        return new KVSharedVersionedBuffer<>(store);
    }
}