/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep.core.pattern;

import com.github.fhuss.kafka.streams.cep.core.Event;
import com.github.fhuss.kafka.streams.cep.core.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.core.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.core.state.ReadOnlySharedVersionBuffer;
import com.github.fhuss.kafka.streams.cep.core.state.States;

/**
 * The context used to matching an event.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class MatcherContext<K, V> {

    private final ReadOnlySharedVersionBuffer<K, V> buffer;
    private final DeweyVersion version;
    private final Stage<K, V> previousStage;
    private final Stage<K, V> currentStage;
    private final Event<K, V> previousEvent;
    private final Event<K, V> currentEvent;
    private final States<K> states;

    public MatcherContext(final ReadOnlySharedVersionBuffer<K, V> buffer,
                          final DeweyVersion version,
                          final Stage<K, V> previousStage,
                          final Stage<K, V> currentStage,
                          final Event<K, V> previousEvent,
                          final Event<K, V> currentEvent,
                          final States<K> states) {
        this.buffer = buffer;
        this.version = version;
        this.previousStage = previousStage;
        this.currentStage = currentStage;
        this.previousEvent = previousEvent;
        this.currentEvent = currentEvent;
        this.states = states;
    }

    public ReadOnlySharedVersionBuffer<K, V> getBuffer() {
        return buffer;
    }

    public DeweyVersion getVersion() {
        return version;
    }

    public Stage<K, V> getPreviousStage() {
        return previousStage;
    }

    public Stage<K, V> getCurrentStage() {
        return currentStage;
    }

    public Event<K, V> getPreviousEvent() {
        return previousEvent;
    }

    public Event<K, V> getCurrentEvent() {
        return currentEvent;
    }

    public States<K> getStates() {
        return states;
    }
}
