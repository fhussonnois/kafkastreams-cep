package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.core.state.SharedVersionedBufferStore;
import org.apache.kafka.streams.processor.StateStore;

public interface SharedVersionedBufferStateStore<K, V> extends SharedVersionedBufferStore<K, V>, StateStore {
}
