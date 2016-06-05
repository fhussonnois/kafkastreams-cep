package com.github.fhuz.kafka.streams.cep;

import java.util.LinkedHashMap;
import java.util.List;

public class Sequence<K, V> {

    /**
     * Lists of events keyed by stage name.
     */
    private LinkedHashMap<String, List<Event<K, V>>> sequence;

    /**
     * Creates a new {@link Sequence} instance.
     * @param sequence
     */
    public Sequence(LinkedHashMap<String, List<Event<K, V>>> sequence) {
        this.sequence = sequence;
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
}
