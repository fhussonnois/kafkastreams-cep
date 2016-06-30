package com.github.fhuz.kafka.streams.cep.demo;

import com.github.fhuz.kafka.streams.cep.CEPProcessor;
import com.github.fhuz.kafka.streams.cep.Sequence;
import com.github.fhuz.kafka.streams.cep.pattern.Pattern;
import com.github.fhuz.kafka.streams.cep.pattern.QueryBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CEPStockKStreamsDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-cep");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, StockEventSerDe.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, StockEventSerDe.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Pattern<Object, StockEvent> pattern = new QueryBuilder<Object, StockEvent>()
                .select()
                    .where((k, v, ts, store) -> v.volume > 1000)
                    .<Long>fold("avg", (k, v, curr) -> v.price)
                    .then()
                .select()
                    .zeroOrMore()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.price > (long)state.get("avg"))
                    .<Long>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                    .<Long>fold("volume", (k, v, curr) -> v.volume)
                    .then()
                .select()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.volume < 0.8 * state.getOrElse("volume", 0L))
                    .within(1, TimeUnit.HOURS)
                .build();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("events", "StockEvents")
                .addProcessor("cep", () -> new CEPProcessor<>(pattern), "events")
                .addProcessor("matches", () -> new AbstractProcessor<Object,  Sequence<Object, StockEvent>>() {
                    @Override
                    public void doProcess(ProcessorContext context, Object key, Sequence<Object, StockEvent> sequence) {
                        JSONObject json = new JSONObject();
                        sequence.asMap().forEach( (k, v) -> {
                            JSONArray events = new JSONArray();
                            json.put(k, events);
                            List<String> collect = v.stream().map(e -> e.value.name).collect(Collectors.toList());
                            Collections.reverse(collect);
                            collect.forEach(e -> events.add(e));
                        });
                        context.forward(null, json.toJSONString());
                    }
                }, "cep")
                .addSink("sink", "matches", Serdes.String().serializer(),Serdes.String().serializer(), "matches");

        //Use the topologyBuilder and streamingConfig to start the kafka streams process
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, props);
        streaming.start();
    }

    static abstract class AbstractProcessor<K, V> implements Processor<K, V> {
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
}
