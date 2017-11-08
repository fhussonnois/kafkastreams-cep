package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.PredicateBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;

/**
 *
 */
public interface CEPStream<K, V> {

    KStream<K, Sequence<K, V>> query(final String queryName, final PredicateBuilder<K, V> builder, final Serde<K> keySerDe, final Serde<V> valueSerde);

    KStream<K, Sequence<K, V>> query(final String queryName, final Pattern<K, V> pattern, final Serde<K> keySerDe, final Serde<V> valueSerde);
}
