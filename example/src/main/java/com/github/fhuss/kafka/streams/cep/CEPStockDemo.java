/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.core.Sequence;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka Streams CEP Demonstration Application.
 *
 * Query :
 *      PATTERN SEQ(Stock+ a[ ], Stock b)
 *           WHERE skip_till_next_match(a[ ], b) {
 *            [symbol]
 *            and
 *            a[1].volume > 1000
 *            and
 *            a[i].price > avg(a[..i-1].price)
 *            and
 *            b.volume < 80%*a[a.LEN].volume
 *            }
 *            WITHIN 1 hour
 *
 */
public class CEPStockDemo {

    private static final Logger LOG = LoggerFactory.getLogger(CEPStockDemo.class);

    public static void main(String[] args) {

        LOG.info("Starting Stocks Application");

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stocks-demo");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockEventSerde.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Topology topology = CEPStockDemo.topology("Stocks", "Stocks", "Matches");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        kafkaStreams.cleanUp();

        kafkaStreams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping Stocks Application");
            kafkaStreams.close();
        }));
    }


    public static Topology topology(final String queryName,
                                    final String inputTopic,
                                    final String outputTopic) {
        // Build query
        ComplexStreamsBuilder builder = new ComplexStreamsBuilder();

        CEPStream<String, StockEvent> stream = builder.stream(inputTopic);
        KStream<String, Sequence<String, StockEvent>> stocks = stream.query(queryName, Patterns.STOCKS);

        stocks.mapValues(seq -> sequenceAsJson(seq))
                .through(outputTopic, Produced.with(null, Serdes.String()))
                .print(Printed.toSysOut());

        return builder.build();
    }

    private static String sequenceAsJson(Sequence<String, StockEvent> seq) {
        JSONObject json = new JSONObject();
        JSONArray events = new JSONArray();
        json.put("events", events);
        seq.matched().forEach( v -> {
            JSONObject stage = new JSONObject();
            stage.put("name", v.getStage());
            stage.put("events", v.getEvents().stream().map(e -> e.value().name).collect(Collectors.toList()));
            events.add(stage);
        });
        return json.toJSONString();
    }
}
