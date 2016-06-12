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
package com.github.fhuz.kafka.streams.cep.pattern;

import com.github.fhuz.kafka.streams.cep.State;
import com.github.fhuz.kafka.streams.cep.nfa.EdgeOperation;
import com.github.fhuz.kafka.streams.cep.nfa.NFA;

import java.util.ArrayList;
import java.util.List;

/**
 * Default class to build a NFA based on a sequence pattern.
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 *
 * @param <K> the type of the key event.
 * @param <V> the type of the value event.
 */
public class NFAFactory<K, V> {

    /**
     * Compiles the specified {@link Pattern}.
     *
     * @param pattern the pattern to make.
     * @return a new {@link NFA} instance.
     */
    public List<State<K, V>> make(Pattern<K, V> pattern) {
        if( pattern == null) throw new NullPointerException("Cannot make null pattern");

        List<State<K, V>> sequence = new ArrayList<>();

        State<K, V> successorState     = new State<>("$final", State.StateType.FINAL);
        sequence.add(successorState);

        Pattern<K, V> successorPattern = null;
        Pattern<K, V> currentPattern   = pattern;

        while( currentPattern.getAncestor() != null) {
            successorState = buildState(State.StateType.NORMAL, currentPattern, successorState, successorPattern);
            sequence.add(successorState);
            successorPattern = currentPattern;
            currentPattern = currentPattern.getAncestor();
        }

        State<K, V> beginState = buildState(State.StateType.BEGIN, currentPattern, successorState, successorPattern);
        sequence.add(beginState);

        return sequence;
    }

    private State<K, V> buildState(State.StateType type, Pattern<K, V> currentPattern, State<K, V> successorState, Pattern<K, V> successorPattern) {

        Pattern.Cardinality cardinality = currentPattern.getCardinality();
        State.StateType currentType = type;

        boolean hasMandatoryState = cardinality.equals(Pattern.Cardinality.ONE_OR_MORE);

        if(hasMandatoryState) currentType = State.StateType.NORMAL;

        State<K, V> state = new State<>(currentPattern.getName(),currentType);
        long windowLengthMs = getWindowLengthMs(currentPattern, successorPattern);
        state.setWindow(windowLengthMs); // Pushing the time window early

        final Matcher<K, V> predicate = currentPattern.getPredicate();
        EdgeOperation operation = cardinality.equals(Pattern.Cardinality.ONE) ? EdgeOperation.BEGIN : EdgeOperation.TAKE;
        state.addEdge(new State.Edge<>(operation, predicate, successorState));

        Pattern.SelectStrategy currentPatternStrategy = currentPattern.getStrategy();

        Matcher<K, V> ignore = null;
        // ignore = true
        if( currentPatternStrategy.equals(Pattern.SelectStrategy.SKIP_TIL_ANY_MATCH) ) {
            ignore = (key, value, ts, store) -> true;
            state.addEdge(new State.Edge<>(EdgeOperation.IGNORE, ignore, null));
        }

        // ignore = !(take)
        if (currentPatternStrategy.equals(Pattern.SelectStrategy.SKIP_TIL_NEXT_MATCH)) {
            ignore = Matcher.not(predicate);
            state.addEdge(new State.Edge<>(EdgeOperation.IGNORE, ignore, null));
        }

        if( operation.equals(EdgeOperation.TAKE) ) {
            // proceed = successor_begin || (!take && !ignore)
            boolean isStrict = currentPatternStrategy.equals(Pattern.SelectStrategy.STRICT_CONTIGUITY);
            Matcher<K, V> proceed =
                    isStrict ? Matcher.or(successorPattern.getPredicate(), Matcher.not(predicate)) :
                            Matcher.or(
                                    successorPattern.getPredicate(),
                                    Matcher.and(Matcher.not(predicate), Matcher.not(ignore)));
            state.addEdge(new State.Edge<>(EdgeOperation.PROCEED, proceed, successorState));
        }

        // we need to introduce a required state
        if(hasMandatoryState) {
            successorState = state;
            state = new State<>(currentPattern.getName(), type);
            state.addEdge(new State.Edge<>(EdgeOperation.BEGIN,  currentPattern.getPredicate(), successorState));

            state.setWindow(windowLengthMs); // Pushing the time window early
        }

        return state;
    }

    private long getWindowLengthMs(Pattern<K, V> currentPattern, Pattern<K, V> successorPattern) {
        if (currentPattern.getWindowTime() != null )
            return currentPattern.getWindowUnit().toMillis(currentPattern.getWindowTime());
        else if( successorPattern!= null && successorPattern.getWindowTime() != null)
            return successorPattern.getWindowUnit().toMillis(successorPattern.getWindowTime());
        return -1;
    }
}
