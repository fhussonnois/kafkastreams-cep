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
package com.github.fhuss.kafka.streams.cep;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Sequence<K, V> {

    /**
     * Lists of events keyed by stage name.
     */
    private LinkedHashMap<String, List<Event<K, V>>> sequence;

    public Sequence() {
        this.sequence = new LinkedHashMap<>();
    }

    /**
     * Creates a new {@link Sequence} instance.
     * @param sequence
     */
    public Sequence(LinkedHashMap<String, List<Event<K, V>>> sequence) {
        this.sequence = sequence;
    }

    public Sequence<K, V> add(String stage, Event<K, V> event) {
        List<Event<K, V>> events = sequence.get(stage);
        if( events == null) {
            events = new ArrayList<>();
            this.sequence.put(stage, events);
        }
        events.add(event);
        return this;
    }

    /**
     * Returns the all events that match the specified stage.
     * @param stage the name of the stage.
     */
    public List<Event<K, V>> get(String stage) {
        return this.sequence.get(stage);
    }

    public Map<String, List<Event<K, V>>> asMap() {
        return this.sequence;
    }

    /**
     * Returns the number of events contains into this sequence.
     */
    public long size() {
        return sequence.values()
                .stream()
                .mapToInt(List::size).sum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sequence<?, ?> that = (Sequence<?, ?>) o;

        for(Map.Entry<String, List<Event<K, V>>> entry : sequence.entrySet()) {
            List<? extends Event<?, ?>> events = that.get(entry.getKey());
            if( events == null) return false;
            if( ! (entry.getValue().size() == events.size() && entry.getValue().containsAll(events))  )
                return false;
        }

        return true;
    }
}
