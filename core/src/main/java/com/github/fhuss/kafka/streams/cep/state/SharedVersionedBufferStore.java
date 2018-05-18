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
package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.Event;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.state.internal.Matched;
import org.apache.kafka.streams.processor.StateStore;

/**
 * A buffer with a compact structure to store partial and complete matches for all runs.
 *
 * The implementation is based on the paper "Efficient Pattern Matching over Event Streams".
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public interface SharedVersionedBufferStore<K , V> extends StateStore {

    /**
     * Adds a new event match into this shared buffer.
     *
     * @param currStage the state for which the event must be added.
     * @param currEvent the current event to add.
     * @param prevStage the predecessor state.
     * @param prevEvent the predecessor event.
     * @param version the predecessor version.
     */
    void put(final Stage<K, V> currStage,
             final Event<K, V> currEvent,
             final Stage<K, V> prevStage,
             final Event<K, V> prevEvent,
             final DeweyVersion version);

    /**
     * Adds a new event match into this shared buffer.
     *
     * @param stage the state on which the event match.
     * @param evt the event.
     * @param version the dewey version attached to this match.
     */
    void put(final Stage<K, V> stage, final Event<K, V> evt, final DeweyVersion version);

    /**
     * Retrieves the complete event sequence for the specified final event.
     *
     * @param matched the sequence of matching events.
     * @param version the final dewey version of the sequence.
     * @return a new {@link Sequence} instance.
     */
    Sequence<K, V> get(final Matched matched, final DeweyVersion version);

    /**
     * Remove all events attached to a sequence.
     *
     * @param matched the sequence of matching events.
     * @param version the final dewey version of the sequence.
     * @return the previous sequence associated with state, event and version, or null if there was no sequence for that.
     */
    Sequence<K, V> remove(final Matched matched, final DeweyVersion version);


    void branch(Stage<K, V> stage, Event<K, V> event, DeweyVersion version);
}
