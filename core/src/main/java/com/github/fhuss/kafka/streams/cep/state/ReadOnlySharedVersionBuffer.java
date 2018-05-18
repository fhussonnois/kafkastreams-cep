package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.state.internal.Matched;

public class ReadOnlySharedVersionBuffer<K, V> {

    private final SharedVersionedBufferStore<K, V> store;

    /**
     * Creates a new {@link ReadOnlySharedVersionBuffer} instance.
     * @param store the inner instance of {@link SharedVersionedBufferStore}.
     */
    public ReadOnlySharedVersionBuffer(final SharedVersionedBufferStore<K, V> store) {
        this.store = store;
    }

    /**
     * Retrieves the complete event sequence for the specified final event.
     *
     * @param matched the final event of the sequence.
     * @param version the final dewey version of the sequence.
     * @return a new {@link Sequence} instance.
     */
    public Sequence<K, V> get(final Matched matched, final DeweyVersion version) {
        return this.store.get(matched, version);
    }
}
