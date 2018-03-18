package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.PredicateBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Default interface to build a CEP Streams.
 */
public interface CEPStream<K, V> {

    /**
     *
     * @param queryName     the query name.
     * @param builder       the builder to builder the pattern to query.
     * @return a {@link KStream} that contains only those records that satisfy the given pattern with unmodified keys and new values.
     */
    default KStream<K, Sequence<K, V>> query(final String queryName,  final PredicateBuilder<K, V> builder) {
        return query(queryName, builder, null);
    }

    /**
     *
     * @param queryName     the query name.
     * @param builder       the builder to builder the pattern to query.
     * @param queried
     * @return a {@link KStream} that contains only those records that satisfy the given pattern with unmodified keys and new values.
     */
    default KStream<K, Sequence<K, V>> query(final String queryName,
                                     final PredicateBuilder<K, V> builder,
                                     final Queried<K, V> queried) {
        return query(queryName, builder.build(), queried);
    }


    /**
     *
     * @param queryName     the query name.
     * @param pattern       the pattern to query.
     * @return a {@link KStream} that contains only those records that satisfy the given pattern with unmodified keys and new values.
     */
    default KStream<K, Sequence<K, V>> query(final String queryName, final Pattern<K, V> pattern) {
        return query(queryName, pattern, null);
    }

    /**
     *
     * @param queryName     the query name.
     * @param pattern       the pattern to query.
     * @param queried
     * @return a {@link KStream} that contains only those records that satisfy the given pattern with unmodified keys and new values.
     */
    KStream<K, Sequence<K, V>> query(final String queryName,
                                     final Pattern<K, V> pattern,
                                     final Queried<K, V> queried);
}
