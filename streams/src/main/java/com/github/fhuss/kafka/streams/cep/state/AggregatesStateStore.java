package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.core.state.AggregatesStore;
import org.apache.kafka.streams.processor.StateStore;

public interface AggregatesStateStore<K> extends AggregatesStore<K>, StateStore {
}
