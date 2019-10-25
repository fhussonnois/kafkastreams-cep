package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.core.state.NFAStore;
import org.apache.kafka.streams.processor.StateStore;

public interface NFAStateStore<K, V> extends NFAStore<K, V>, StateStore {
}
