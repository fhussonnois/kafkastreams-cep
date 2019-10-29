package com.github.fhuss.kafka.streams.cep.state.internal;

import com.github.fhuss.kafka.streams.cep.Queried;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;

import java.util.Map;

public class QueriedInternal<K, V> extends Queried<K, V> {

    public QueriedInternal() {
        super();
    }

    public QueriedInternal(final Queried<K, V> queried) {
        super(queried);
    }

    public StoreSupplier<KeyValueStore<Bytes, byte[]>>  storeSupplier() {
        return storeSupplier;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    public boolean cachingEnabled() {
        return cachingEnabled;
    }

    public Map<String, String> logConfig() {
        return topicConfig;
    }


}
