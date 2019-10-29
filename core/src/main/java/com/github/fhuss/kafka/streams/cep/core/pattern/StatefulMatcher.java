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
import com.github.fhuss.kafka.streams.cep.core.state.States;

/**
 * A matcher defines the condition under which an event should be selected to be added to the pattern sequence.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
@FunctionalInterface
public interface StatefulMatcher<K, V> extends Matcher<K, V> {

    /**
     * {@inheritDoc}
     */
    @Override
    default boolean accept(MatcherContext<K, V> context) {
        return matches(context.getCurrentEvent(), context.getStates());
    }

    /**
     * The function that evaluates an input record stream.
     *
     * @param event         the current event in the stream.
     * @param states        the states store for this pattern.
     * @return <code>true</code> if the event should be selected.
     */
    boolean matches(final Event<K, V> event, final States states);
}
