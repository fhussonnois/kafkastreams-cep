package com.github.fhuz.kafka.streams.cep;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    /**
     * Returns the number of events contains into this sequence.
     */
    public long size() {
        return sequence.values()
                .stream()
                .flatMap(List::stream)
                .count();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sequence<?, ?> that = (Sequence<?, ?>) o;

        for(Map.Entry<String, List<Event<K, V>>> entry : sequence.entrySet()) {
            List<? extends Event<?, ?>> events = that.get(entry.getKey());
            if( events == null) return false;
            if( ! entry.getValue().equals(events) ) return false;
        }

        return true;
    }
}
