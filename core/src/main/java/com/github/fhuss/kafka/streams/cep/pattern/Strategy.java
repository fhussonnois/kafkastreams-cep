package com.github.fhuss.kafka.streams.cep.pattern;

/**
 * Strategies that can be used to select events.
 */
public enum Strategy {
    /**
     * This strategy enforce the selected events to be contiguous in the input stream.
     */
    STRICT_CONTIGUITY,
    /**
     * Irrelevant events are skipped until an event matching the next pattern is encountered.
     * If multiple events in the stream can match the next pattern only the first of them is selected.
     */
    SKIP_TIL_NEXT_MATCH,
    /**
     * Irrelevant events are skipped until an event matching the next pattern is encountered.
     * All events in the stream that can match a pattern are selected.
     */
    SKIP_TIL_ANY_MATCH
}