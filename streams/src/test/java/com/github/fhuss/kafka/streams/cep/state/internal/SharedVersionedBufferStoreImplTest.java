package com.github.fhuss.kafka.streams.cep.state.internal;

import com.github.fhuss.kafka.streams.cep.core.Event;
import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.core.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.core.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.core.state.SharedVersionedBufferStore;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Matched;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStateStore;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SharedVersionedBufferStoreImplTest {

    private static final String TOPIC_TEST = "topic-test";

    private static Event<String, String> ev1 = new Event<>("k1", "v1", 1000000001L, TOPIC_TEST, 0, 0L);
    private static Event<String, String> ev2 = new Event<>("k2", "v2", 1000000002L, TOPIC_TEST, 0, 1L);
    private static Event<String, String> ev3 = new Event<>("k3", "v3", 1000000003L, TOPIC_TEST, 0, 2L);
    private static Event<String, String> ev4 = new Event<>("k4", "v4", 1000000004L, TOPIC_TEST, 0, 3L);
    private static Event<String, String> ev5 = new Event<>("k5", "v5", 1000000005L, TOPIC_TEST, 0, 4L);

    private static Stage<String, String> first  = new Stage<>(0, "first", Stage.StateType.BEGIN);
    private static Stage<String, String> second = new Stage<>(1, "second", Stage.StateType.NORMAL);
    private static Stage<String, String> latest = new Stage<>(2, "latest", Stage.StateType.FINAL);

    private SharedVersionedBufferStore<String, String> buffer  = this.getInMemorySharedBuffer(Serdes.String(), Serdes.String());

    @Test
    public void testExtractPatternsWithOneRun() {
        buffer.put(first, ev1, new DeweyVersion("1"));
        buffer.put(second, ev2, first, ev1, new DeweyVersion("1.0"));
        buffer.put(latest, ev3, second, ev2, new DeweyVersion("1.0.0"));

        Sequence<String, String> sequence = buffer.get(Matched.from(latest, ev3), new DeweyVersion("1.0.0"));
        assertNotNull(sequence);
        assertEquals(3, sequence.size());
        assertEquals(sequence.getByName("latest").getEvents().iterator().next(), ev3);
        assertEquals(sequence.getByName("second").getEvents().iterator().next(), ev2);
        assertEquals(sequence.getByName("first").getEvents().iterator().next(), ev1);
    }

    @Test
    public void testExtractPatternsWithBranchingRun() {
        buffer.put(first, ev1, new DeweyVersion("1"));
        buffer.put(second, ev2, first, ev1, new DeweyVersion("1.0"));
        buffer.put(latest, ev3, second, ev2, new DeweyVersion("1.0.0"));

        buffer.put(second, ev3, second, ev2,  new DeweyVersion("1.1"));
        buffer.put(second, ev4, second, ev3, new DeweyVersion("1.1"));
        buffer.put(latest, ev5, second, ev4, new DeweyVersion("1.1.0"));

        Sequence<String, String> sequence1 = buffer.get(Matched.from(latest, ev3), new DeweyVersion("1.0.0"));
        assertNotNull(sequence1);
        assertEquals(3, sequence1.size());
        assertEquals(sequence1.getByName("latest").getEvents().iterator().next(), ev3);
        assertEquals(sequence1.getByName("second").getEvents().iterator().next(), ev2);
        assertEquals(sequence1.getByName("first").getEvents().iterator().next(), ev1);

        Sequence<String, String> sequence2 = buffer.get(Matched.from(latest, ev5), new DeweyVersion("1.1.0"));
        assertNotNull(sequence2);
        assertEquals(5, sequence2.size());
        assertEquals(1, sequence2.getByName("latest").getEvents().size());
        assertEquals(3, sequence2.getByName("second").getEvents().size());
        assertEquals(1, sequence2.getByName("first").getEvents().size());
    }

    @SuppressWarnings("unchecked")
    private <K, V> SharedVersionedBufferStore<K, V> getInMemorySharedBuffer(Serde<K> keySerDe, Serde<V> valueSerDe) {
        InMemoryKeyValueStore test = new InMemoryKeyValueStore("test");
        SharedVersionedBufferStateStore<K, V> store =  new SharedVersionedBufferStoreImpl<>(test, keySerDe, valueSerDe);
        store.init(new NoOpProcessorContext(), null);
        return store;
    }
}