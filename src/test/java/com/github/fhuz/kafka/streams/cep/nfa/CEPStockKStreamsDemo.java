package com.github.fhuz.kafka.streams.cep.nfa;


import com.github.fhuz.kafka.streams.cep.CEPProcessor;
import com.github.fhuz.kafka.streams.cep.Sequence;
import com.github.fhuz.kafka.streams.cep.serde.KryoSerDe;
import com.github.fhuz.kafka.streams.cep.pattern.Pattern;
import com.github.fhuz.kafka.streams.cep.pattern.QueryBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CEPStockKStreamsDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-cep");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Pattern<Object, StockEvent> pattern = new QueryBuilder<Object, StockEvent>()
                .select()
                    .where((k, v, ts, store) -> v.volume > 1000)
                    .<Integer>fold("avg", (k, v, curr) -> v.price)
                    .then()
                .select()
                    .zeroOrMore()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.price > (int)state.get("avg"))
                    .<Integer>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                    .<Integer>fold("volume", (k, v, curr) -> v.volume)
                    .then()
                .select()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.volume < 0.8 * state.getOrElse("volume", 0))
                .within(1, TimeUnit.HOURS)
                .build();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("source", "StockEvents")
                .addProcessor("map", () -> new AbstractProcessor<String, String>() {
                    @Override
                    public void doProcess(ProcessorContext context, String key, String value) {
                        try {
                            String[] split = value.split(",");
                            context.forward(null, new StockEvent(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
                        } catch (NumberFormatException e) {
                            //ignore
                        }
                    }
                }, "source")
                .addProcessor("cep", () -> new CEPProcessor<>(pattern), "map")
                .addProcessor("print", () -> new AbstractProcessor<Object,  Sequence<Object, StockEvent>>() {
                    @Override
                    public void doProcess(ProcessorContext context, Object key, Sequence<Object, StockEvent> sequence) {
                        sequence.asMap().forEach( (k, v) -> {
                            System.out.println(k + "->");
                            System.out.println(v);
                        });
                    }
                }, "cep")
                .addSink("sink", "hello", "print");

        // Required for buffer event matches
        topologyBuilder.addStateStore(CEPProcessor.getEventsStore(true), "cep");

        // Aggregates states (i.e fold method)
        topologyBuilder.addStateStore(newStateStore("volume", true), "cep");
        topologyBuilder.addStateStore(newStateStore("avg", true), "cep");

        //Use the topologyBuilder and streamingConfig to start the kafka streams process
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, props);
        streaming.start();
    }

    public static abstract class AbstractProcessor<K, V> implements Processor<K, V> {
        private ProcessorContext context;
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(K key, V value) {
            doProcess(context, key, value);
        }

        public abstract void doProcess(ProcessorContext context, K key, V value);

        @Override
        public void punctuate(long timestamp) {

        }

        @Override
        public void close() {

        }
    }

    public static StateStoreSupplier newStateStore(String name, boolean isMemory) {
        KryoSerDe serde = new KryoSerDe();
        Stores.KeyValueFactory factory = Stores.create(name)
                .withKeys(serde)
                .withValues(serde);
        return isMemory ? factory.inMemory().build() : factory.persistent().build();
    }

    public static class StockEvent {
        public int price;
        public int volume;

        /**
         * Dummy constructor (Kryo)
         */
        public StockEvent(){}

        public StockEvent(int price, int volume) {
            this.price = price;
            this.volume = volume;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("StockEvent{");
            sb.append("price=").append(price);
            sb.append(", volume=").append(volume);
            sb.append('}');
            return sb.toString();
        }
    }
}
