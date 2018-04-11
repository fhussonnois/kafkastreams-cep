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
package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CEPStreamIntegrationTest {

    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final String APPLICATION_ID       = "streams-cep";
    private static final String INPUT_STREAM_1_P2    = "input_t1_p2";
    private static final String OUTPUT_STREAM_1_P2   = "output_t1_p2";


    private static final Pattern<String, Integer> SIMPLE_PATTERN = new QueryBuilder<String, Integer>()
            .select("stage-1")
            .where((k, v, ts, store) -> v == 0)
            .<Integer>fold("sum", (k, v, curr) -> v)
            .then()
            .select("stage-2")
            .oneOrMore()
            .where((k, v, ts, store) -> ((int) store.get("sum")) <= 10)
            .<Integer>fold("sum", (k, v, curr) -> curr + v)
            .then()
            .select("stage-3")
            .where((k, v, ts, store) -> ((int) store.get("sum")) + v > 10)
            .within(1, TimeUnit.HOURS)
            .build();

    private static final String K1 = "K1";
    private static final String K2 = "K2";

    private static final String STAGE_1 = "stage-1";
    private static final String STAGE_2 = "stage-2";
    private static final String STAGE_3 = "stage-3";

    private final MockTime mockTime = CLUSTER.time;

    private Properties streamsConfiguration = new Properties();

    private KafkaStreams kafkaStreams;

    @Before
    public void before() {
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("test").getPath());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void testWithMultipleKey() throws ExecutionException, InterruptedException {
        CLUSTER.createTopic(INPUT_STREAM_1_P2, 2, NUM_BROKERS);
        CLUSTER.createTopic(OUTPUT_STREAM_1_P2, 2, NUM_BROKERS);

        final Collection<KeyValue<String, Integer>> batch1 = Arrays.asList(
                new KeyValue<>(K1, 0),
                new KeyValue<>(K2, -10),
                new KeyValue<>(K2, 0),
                new KeyValue<>(K1, 3),
                new KeyValue<>(K2, 6),
                new KeyValue<>(K1, 1),
                new KeyValue<>(K1, 2),
                new KeyValue<>(K1, 6),
                new KeyValue<>(K2, 4),
                new KeyValue<>(K2, 4)
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_STREAM_1_P2,
                batch1,
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        IntegerSerializer.class,
                        new Properties()),
                mockTime);

        // Build query
        ComplexStreamsBuilder builder = new ComplexStreamsBuilder();

        CEPStream<String, Integer> stream = builder.stream(INPUT_STREAM_1_P2, Consumed.with(Serdes.String(), Serdes.Integer()));
        KStream<String, Sequence<String, Integer>> sequences = stream.query("query-test-1", SIMPLE_PATTERN,
                Queried.with(Serdes.String(), Serdes.Integer()));

        sequences.to(OUTPUT_STREAM_1_P2, Produced.with(Serdes.String(), new JsonSequenceSerde<>()));

        Topology topology = builder.build();
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        kafkaStreams.start();

        final Properties consumerConfig = TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                StringDeserializer.class,
                JsonSequenceSerde.SequenceDeserializer.class);

        // JSON values are de-serialized as double
        List<KeyValue<String, Sequence<String, Double>>> result = IntegrationTestUtils.readKeyValues(
                OUTPUT_STREAM_1_P2, consumerConfig,
                TimeUnit.SECONDS.toMillis(10), 2);

        Assert.assertEquals(2, result.size());
        final KeyValue<String, Sequence<String, Double>> kvOne = result.get(0);
        Assert.assertEquals(K1, kvOne.key);
        assertStagesNames(kvOne.value, STAGE_1, STAGE_2, STAGE_3);
        assertStagesValue(kvOne.value, STAGE_1, 0.0);
        assertStagesValue(kvOne.value, STAGE_2, 3.0, 1.0, 2.0);
        assertStagesValue(kvOne.value, STAGE_3, 6.0);


        final KeyValue<String, Sequence<String, Double>> kvTwo = result.get(1);
        Assert.assertEquals(K2, kvTwo.key);
        assertStagesNames(kvTwo.value, STAGE_1, STAGE_2, STAGE_3);
        assertStagesValue(kvTwo.value, STAGE_1, 0.0);
        assertStagesValue(kvTwo.value, STAGE_2, 6.0, 4.0);
        assertStagesValue(kvTwo.value, STAGE_3, 4.0);
    }

    private <K, V> void assertStagesNames(Sequence<K, V> sequence, String...stages) {
        List<String> expected = Arrays.asList(stages);
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i), sequence.getByIndex(i).getStage());
        }
    }

    private <K, V> void assertStagesValue(Sequence<K, V> sequence, String stage, V...values) {
        List<V> expected = Arrays.asList(values);
        Sequence.Staged<K, V> staged = sequence.getByName(stage);
        List<Event<K, V>> events = new ArrayList<>(staged.getEvents());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i).toString(), events.get(i).value.toString());
        }
    }


    @After
    public void shutdown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close(30, TimeUnit.SECONDS);
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }
}