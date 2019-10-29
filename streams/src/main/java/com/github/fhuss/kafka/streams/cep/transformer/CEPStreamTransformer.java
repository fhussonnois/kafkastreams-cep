package com.github.fhuss.kafka.streams.cep.transformer;

import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.processor.CEPProcessor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

public class CEPStreamTransformer<K, V> implements ValueTransformerWithKey<K, V, Iterable<Sequence<K, V>>> {

    private CEPProcessor<K, V> internal;

    /**
     * Creates a new {@link CEPStreamTransformer} instance.
     * @param queryName     the query name.
     * @param queryPattern  the query {@link Pattern}.
     */
    public CEPStreamTransformer(final String queryName,
                                final Pattern<K, V> queryPattern) {
        internal = new CEPProcessor<>(queryName, queryPattern);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final ProcessorContext context) {
        internal.init(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Sequence<K, V>> transform(final K k, final V v) {
        return internal.match(k, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        internal.close();
    }
}
