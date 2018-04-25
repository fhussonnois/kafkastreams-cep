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
package com.github.fhuss.kafka.streams.cep.pattern;

import com.github.fhuss.kafka.streams.cep.nfa.EdgeOperation;
import com.github.fhuss.kafka.streams.cep.nfa.NFA;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default class to build all states based on a sequence pattern.
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 *
 * @param <K> the type of the key event.
 * @param <V> the type of the value event.
 */
public class StagesFactory<K, V> {

    private final AtomicInteger stageId = new AtomicInteger(0);

    /**
     * Compiles the specified {@link Pattern}.
     *
     * @param pattern the pattern to make.
     * @return a new {@link NFA} instance.
     */
    public List<Stage<K, V>> make(final Pattern<K, V> pattern) {
        if( pattern == null) throw new NullPointerException("Cannot make null pattern");

        final List<Stage<K, V>> sequence = new ArrayList<>();

        Stage<K, V> successorStage = new Stage<>(nextStageId(), "$final", Stage.StateType.FINAL);
        sequence.add(successorStage);

        Pattern<K, V> successorPattern = null;
        Pattern<K, V> currentPattern   = pattern;

        while( currentPattern.getAncestor() != null) {
            List<Stage<K, V>> stages = buildStages(Stage.StateType.NORMAL, currentPattern, successorStage, successorPattern);
            sequence.addAll(stages);
            successorStage = stages.get(stages.size() - 1);
            successorPattern = currentPattern;
            currentPattern = currentPattern.getAncestor();
        }
        sequence.addAll(buildStages(Stage.StateType.BEGIN, currentPattern, successorStage, successorPattern));

        return sequence;
    }

    private int nextStageId() {
        return stageId.getAndIncrement();
    }

    @SuppressWarnings("unchecked")
    private List<Stage<K, V>> buildStages(final Stage.StateType type,
                                   final Pattern<K, V> currentPattern,
                                   final Stage<K, V> successorStage,
                                   final Pattern<K, V> successorPattern) {

        final Pattern.Cardinality cardinality = currentPattern.getCardinality();
        Stage.StateType currentType = type;

        final boolean hasMandatoryState = cardinality.equals(Pattern.Cardinality.ONE_OR_MORE);

        if (hasMandatoryState) currentType = Stage.StateType.NORMAL;

        Stage<K, V> stage = new Stage<>(nextStageId(), currentPattern.getName(),currentType);
        long windowLengthMs = getWindowLengthMs(currentPattern, successorPattern);
        stage.setWindow(windowLengthMs); // Pushing the time window early
        stage.setAggregates(currentPattern.getAggregates());

        final Selected selected = currentPattern.getSelected();

        final Matcher<K, V> predicate = currentPattern.getPredicate();

        EdgeOperation operation = cardinality.equals(Pattern.Cardinality.ONE) ? EdgeOperation.BEGIN : EdgeOperation.TAKE;
        stage.addEdge(new Stage.Edge<>(operation, predicate, successorStage));

        Matcher<K, V> ignore = null;
        // ignore = true
        if (selected.getStrategy().equals(Strategy.SKIP_TIL_ANY_MATCH) ) {
            ignore = new Matcher.TruePredicate<>();
            stage.addEdge(new Stage.Edge<>(EdgeOperation.IGNORE, ignore, null));
        }

        // ignore = !(take)
        if (selected.getStrategy().equals(Strategy.SKIP_TIL_NEXT_MATCH)) {
            ignore = Matcher.not(predicate);
            stage.addEdge(new Stage.Edge<>(EdgeOperation.IGNORE, ignore, null));
        }

        if (operation.equals(EdgeOperation.TAKE) ) {
            // proceed = successor_begin || (!take && !ignore)
            boolean isStrict = selected.getStrategy().equals(Strategy.STRICT_CONTIGUITY);

            Matcher<K, V> successorPredicate = successorPattern.getPredicate();

            Matcher<K, V> proceed =
                    isStrict ? Matcher.or(successorPredicate, Matcher.not(predicate)) :
                            Matcher.or(
                                    successorPredicate,
                                    Matcher.and(Matcher.not(predicate), Matcher.not(ignore)));
            stage.addEdge(new Stage.Edge<>(EdgeOperation.PROCEED, proceed, successorStage));
        }

        List<Stage<K, V>> stages = new ArrayList<>();
        stages.add(stage);
        // we need to introduce a required state
        if (hasMandatoryState) {
            final Stage internalStage = new Stage<>(nextStageId(), currentPattern.getName(), type);
            internalStage.addEdge(new Stage.Edge<>(EdgeOperation.BEGIN, currentPattern.getPredicate(), stage));
            if (ignore != null) {
                internalStage.addEdge(new Stage.Edge<>(EdgeOperation.IGNORE, ignore, null));
            }
            internalStage.setWindow(windowLengthMs); // Pushing the time window early
            internalStage.setAggregates(currentPattern.getAggregates());
            stages.add(internalStage);
        }
        return stages;
    }

    private long getWindowLengthMs(Pattern<K, V> currentPattern, Pattern<K, V> successorPattern) {
        if (currentPattern.getWindowTime() != null )
            return currentPattern.getWindowUnit().toMillis(currentPattern.getWindowTime());
        else if( successorPattern!= null && successorPattern.getWindowTime() != null)
            return successorPattern.getWindowUnit().toMillis(successorPattern.getWindowTime());
        return -1;
    }
}
