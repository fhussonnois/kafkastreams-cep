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
package com.github.fhuz.kafka.streams.cep.nfa.buffer;

import com.github.fhuz.kafka.streams.cep.Sequence;
import com.github.fhuz.kafka.streams.cep.State;
import com.github.fhuz.kafka.streams.cep.nfa.DeweyVersion;
import com.github.fhuz.kafka.streams.cep.Event;

/**
 * A buffer with a compact structure to store partial and complete matches for all runs.
 *
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 */
public interface SharedVersionedBuffer<K , V> {

    /**
     * Adds a new event match into this shared buffer.
     *
     * @param currState the state for which the event must be added.
     * @param currEvent the current event to add.
     * @param prevState the predecessor state.
     * @param prevEvent the predecessor event.
     * @param version the predecessor version.
     */
    void put(State<K, V> currState, Event<K, V> currEvent, State<K, V> prevState, Event<K, V> prevEvent, DeweyVersion version);

    /**
     * Adds a new event match into this shared buffer.
     *
     * @param state the state on which the event match.
     * @param evt the event.
     * @param version the dewey version attached to this match.
     */
    void put(State<K, V> state, Event<K, V> evt, DeweyVersion version);

    /**
     * Retrieves the complete event sequence for the specified final event.
     *
     * @param state the final state of the sequence.
     * @param event the final event of the sequence.
     * @param version the final dewey version of the sequence.
     * @return a new {@link Sequence} instance.
     */
    Sequence<K, V> get(final State<K, V> state, final Event<K, V> event, final DeweyVersion version);

    /**
     * Remove all events attached to a sequence.
     *
     * @param state the final state of the sequence.
     * @param event the final event of the sequence.
     * @param version the final dewey version of the sequence.
     */
    void remove(final State<K, V> state, final Event<K, V> event, final DeweyVersion version);
}
