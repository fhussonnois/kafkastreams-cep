/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep.demo;

import com.github.fhuss.kafka.streams.cep.CEPStream;
import com.github.fhuss.kafka.streams.cep.ComplexStreamsBuilder;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CEPStockKStreamsIntegrationTest {

    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    public static final String APPLICATION_ID = "streams-cep";
    public static final String INPUT_STREAM = "stock-events";
    public static final String OUTPUT_STREAM = "sequences";
    private final MockTime mockTime = CLUSTER.time;

    private Properties streamsConfiguration = new Properties();

    private KafkaStreams kafkaStreams;

    @Before
    public void before() throws InterruptedException {

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockEventSerDe.class);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("st-test").getPath());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        CLUSTER.createTopic(INPUT_STREAM, 1, NUM_BROKERS);
        CLUSTER.createTopic(OUTPUT_STREAM, 1, NUM_BROKERS);
    }

    @Test
    public void test( ) throws ExecutionException, InterruptedException {

        final Collection<KeyValue<String, String>> batch1 = Arrays.asList(
                new KeyValue<>(null, "{\"name\":\"e1\",\"price\":100,\"volume\":1010}"),
                new KeyValue<>(null, "{\"name\":\"e2\",\"price\":120,\"volume\":990}"),
                new KeyValue<>(null, "{\"name\":\"e3\",\"price\":120,\"volume\":1005}"),
                new KeyValue<>(null, "{\"name\":\"e4\",\"price\":121,\"volume\":999}"),
                new KeyValue<>(null, "{\"name\":\"e5\",\"price\":120,\"volume\":999}"),
                new KeyValue<>(null, "{\"name\":\"e6\",\"price\":125,\"volume\":750}"),
                new KeyValue<>(null, "{\"name\":\"e7\",\"price\":120,\"volume\":950}"),
                new KeyValue<>(null, "{\"name\":\"e8\",\"price\":120,\"volume\":700}")
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_STREAM,
                batch1,
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                mockTime);

        // build query
        final Pattern<String, StockEvent> pattern = new QueryBuilder<String, StockEvent>()
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

        ComplexStreamsBuilder builder = new ComplexStreamsBuilder();

        CEPStream<String, StockEvent> stream = builder.stream(INPUT_STREAM);
        KStream<String, Sequence<String, StockEvent>> stocks = stream.query("Stocks", pattern, Serdes.String(), new StockEventSerDe());

        stocks.mapValues(seq -> {
                  JSONObject json = new JSONObject();
                  seq.asMap().forEach( (k, v) -> {
                      JSONArray events = new JSONArray();
                      json.put(k, events);
                      List<String> collect = v.stream().map(e -> e.value.name).collect(Collectors.toList());
                      Collections.reverse(collect);
                      collect.forEach(events::add);
                  });
                  return json.toJSONString();
              })
              .through(OUTPUT_STREAM, Produced.with(null, Serdes.String()))
              .print(Printed.toSysOut());

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        final Properties consumerConfig = TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                StringDeserializer.class,
                StringDeserializer.class);

        List<KeyValue<String, String>> result = IntegrationTestUtils.readKeyValues(
                OUTPUT_STREAM, consumerConfig,
                TimeUnit.SECONDS.toMillis(10), 4);

        Assert.assertEquals(4, result.size());
        Assert.assertEquals("{\"0\":[\"e1\"],\"1\":[\"e2\",\"e3\",\"e4\",\"e5\"],\"2\":[\"e6\"]}", result.get(0).value);
        Assert.assertEquals("{\"0\":[\"e3\"],\"1\":[\"e4\"],\"2\":[\"e6\"]}",result.get(1).value);
        Assert.assertEquals("{\"0\":[\"e1\"],\"1\":[\"e2\",\"e3\",\"e4\",\"e5\",\"e6\",\"e7\"],\"2\":[\"e8\"]}",result.get(2).value);
        Assert.assertEquals("{\"0\":[\"e3\"],\"1\":[\"e4\",\"e6\"],\"2\":[\"e8\"]}",result.get(3).value);
    }

    @After
    public void shutdown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close(30, TimeUnit.SECONDS);
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }
}
