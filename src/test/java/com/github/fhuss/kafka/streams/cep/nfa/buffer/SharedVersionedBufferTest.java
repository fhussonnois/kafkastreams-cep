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
package com.github.fhuss.kafka.streams.cep.nfa.buffer;

import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.TimedKeyValue;
import com.github.fhuss.kafka.streams.cep.serde.KryoSerDe;
import com.github.fhuss.kafka.streams.cep.Event;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.KVSharedVersionedBuffer;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.StackEventKey;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.TimedKeyValueSerDes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
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
        KVSharedVersionedBuffer<String, String> buffer = this.getInMemorySharedBuffer(Serdes.String(), Serdes.String());
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
        KVSharedVersionedBuffer<String, String> buffer = this.getInMemorySharedBuffer(Serdes.String(), Serdes.String());

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
    private <K, V> KVSharedVersionedBuffer<K, V> getInMemorySharedBuffer(Serde<K> keySerDe, Serde<V> valueSerDe) {
        TimedKeyValueSerDes<K, V> keyValueSerDes = new TimedKeyValueSerDes<>(keySerDe, valueSerDe);
        KryoSerDe<StackEventKey> kryoSerDe = new KryoSerDe<>();
        KeyValueStore<StackEventKey, TimedKeyValue<K, V>> store = new InMemoryKeyValueStore<>("test", kryoSerDe, Serdes.serdeFrom(keyValueSerDes, keyValueSerDes));
        return new KVSharedVersionedBuffer<>(store);
    }
}