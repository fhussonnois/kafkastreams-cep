package com.github.fhuss.kafka.streams.cep.core.state.internal;

public interface MatchedEventStore<K, V> {

    MatchedEvent<K, V> get(final Matched matched);

    void put(final Matched matched, final MatchedEvent<K, V> event);

    void delete(final Matched matched);

}
