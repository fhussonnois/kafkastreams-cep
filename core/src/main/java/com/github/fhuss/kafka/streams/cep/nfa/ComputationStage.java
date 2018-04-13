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
package com.github.fhuss.kafka.streams.cep.nfa;

import com.github.fhuss.kafka.streams.cep.Event;

import java.util.List;

/**
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 *
 * @param <K> the type of the key event.
 * @param <V> the type of the value event.
 */
public class ComputationStage<K, V> {

    /**
     * The name of stage.
     */
    private final String name;

    private Stage<K , V> stage;

    /**
     * The pointer to the most recent event into the share buffer.
     */
    private final Event<K, V> event;

    /**
     * Timestamp of the first event for this pattern.
     */
    private final long timestamp;

    /**
     * The version number.
     */
    private final DeweyVersion version;


    private final long sequence;

    /**
     * Flag to indicate this computation stage is the first of a new branch.
     */
    private final boolean isBranching;

    /**
     * Creates a new {@link ComputationStage} instance.
     * @param version
     * @param event
     * @param timestamp
     * @param sequence
     * @param isBranching
     */
    ComputationStage(final String name,
                     final DeweyVersion version,
                     final Event<K, V> event,
                     final long timestamp,
                     final long sequence,
                     final boolean isBranching) {
        this.name = name;
        this.event = event;
        this.timestamp = timestamp;
        this.version = version;
        this.sequence = sequence;
        this.isBranching = isBranching;
    }

    /**
     * Creates a new {@link ComputationStage} instance.
     * @param stage
     * @param version
     * @param event
     * @param timestamp
     * @param sequence
     * @param isBranching
     */
    ComputationStage(final Stage<K, V> stage,
                     final DeweyVersion version,
                     final Event<K, V> event,
                     final long timestamp,
                     final long sequence,
                     final boolean isBranching) {
        this.name = stage.getName();
        this.stage = stage;
        this.event = event;
        this.timestamp = timestamp;
        this.version = version;
        this.sequence = sequence;
        this.isBranching = isBranching;
    }

    /**
     * Creates a new {@link ComputationStage} for the specified {@link DeweyVersion}.
     * @param version
     * @return
     */
    public ComputationStage<K, V> setVersion(final DeweyVersion version) {
        return new ComputationStageBuilder<K, V>()
                .setStage(stage)
                .setVersion(version)
                .setEvent(event)
                .setTimestamp(timestamp)
                .setSequence(sequence)
                .build();
    }

    public long getSequence() {
        return sequence;
    }


    public boolean isBranching() {
        return isBranching;
    }

    boolean isOutOfWindow(long time) {
        return stage.getWindowMs() != -1 && (time - timestamp) > stage.getWindowMs();
    }

    /**
     * @see {@link Stage#isBeginState()}.
     */
    public boolean isBeginState() {
        return stage.isBeginState();
    }

    /**
     * Checks whether this {@link ComputationStage} is forwarding to the next state.
     * @return <code>true</code> if this computation contains a single "proceed" operation.
     */
    boolean isForwarding() {
        List<Stage.Edge<K, V>> edges = stage.getEdges();
        return ( edges.size() == 1 && edges.get(0).is(EdgeOperation.PROCEED));
    }

    /**
     * Checks whether this {@link ComputationStage} is forwarding to the final state.
     */
    boolean isForwardingToFinalState() {
        List<Stage.Edge<K, V>> edges = stage.getEdges();
        return ( isForwarding()
                && edges.get(0).getTarget().isFinalState());
    }

    public Stage<K, V> getStage() {
        return stage;
    }

    public Event<K, V> getEvent() {
        return event;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public DeweyVersion getVersion() {
        return version;
    }

    public ComputationStage<K, V> setEvent(Event<K, V> event) {
        return new ComputationStageBuilder<K, V>()
                .setStage(stage)
                .setVersion(version)
                .setEvent(event)
                .setTimestamp(timestamp)
                .setSequence(sequence)
                .build();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ComputationStage{");
        sb.append("stage=").append(stage);
        sb.append(", event=").append(event);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }
}
