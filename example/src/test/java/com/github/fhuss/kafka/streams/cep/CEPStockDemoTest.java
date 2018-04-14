/*
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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CEPStockDemoTest {

    private static final String APPLICATION_ID       = "streams-cep";
    private static final String INPUT_STREAM         = "stock-events";
    private static final String OUTPUT_STREAM        = "sequences";

    private static final String K1 = "K1";

    private static final String E1 = "{\"name\":\"e1\",\"price\":100,\"volume\":1010}";
    private static final String E2 = "{\"name\":\"e2\",\"price\":120,\"volume\":990}";
    private static final String E3 = "{\"name\":\"e3\",\"price\":120,\"volume\":1005}";
    private static final String E4 = "{\"name\":\"e4\",\"price\":121,\"volume\":999}";
    private static final String E5 = "{\"name\":\"e5\",\"price\":120,\"volume\":999}";
    private static final String E6 = "{\"name\":\"e6\",\"price\":125,\"volume\":750}";
    private static final String E7 = "{\"name\":\"e7\",\"price\":120,\"volume\":950}";
    private static final String E8 = "{\"name\":\"e8\",\"price\":120,\"volume\":700}";

    private Properties streamsConfiguration = new Properties();

    private ProcessorTopologyTestDriver driver;
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Before
    public void before() {
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("test").getPath());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockEventSerde.class);

        // Create the topology to start testing
        Topology topology = CEPStockDemo.topology("Stocks", INPUT_STREAM, OUTPUT_STREAM);

        StreamsConfig config = new StreamsConfig(streamsConfiguration);
        driver = new ProcessorTopologyTestDriver(config, topology);
    }

    @After
    public void after() throws IOException {
        driver.close();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void test( ) {

        driver.process(INPUT_STREAM, K1, E1, STRING_SERDE.serializer(), STRING_SERDE.serializer());
        driver.process(INPUT_STREAM, K1, E2, STRING_SERDE.serializer(), STRING_SERDE.serializer());
        driver.process(INPUT_STREAM, K1, E3, STRING_SERDE.serializer(), STRING_SERDE.serializer());
        driver.process(INPUT_STREAM, K1, E4, STRING_SERDE.serializer(), STRING_SERDE.serializer());
        driver.process(INPUT_STREAM, K1, E5, STRING_SERDE.serializer(), STRING_SERDE.serializer());
        driver.process(INPUT_STREAM, K1, E6, STRING_SERDE.serializer(), STRING_SERDE.serializer());
        driver.process(INPUT_STREAM, K1, E7, STRING_SERDE.serializer(), STRING_SERDE.serializer());
        driver.process(INPUT_STREAM, K1, E8, STRING_SERDE.serializer(), STRING_SERDE.serializer());

        final List<ProducerRecord> expected = new ArrayList<>();
        expected.add(new ProducerRecord<>(OUTPUT_STREAM, K1,
                "{\"events\":[{\"name\":\"stage-1\",\"events\":[\"e1\"]},{\"name\":\"stage-2\",\"events\":[\"e2\",\"e3\",\"e4\",\"e5\"]},{\"name\":\"stage-3\",\"events\":[\"e6\"]}]}"));
        expected.add(new ProducerRecord<>(OUTPUT_STREAM, K1,
                "{\"events\":[{\"name\":\"stage-1\",\"events\":[\"e3\"]},{\"name\":\"stage-2\",\"events\":[\"e4\"]},{\"name\":\"stage-3\",\"events\":[\"e6\"]}]}"));
        expected.add(new ProducerRecord<>(OUTPUT_STREAM, K1,
                "{\"events\":[{\"name\":\"stage-1\",\"events\":[\"e1\"]},{\"name\":\"stage-2\",\"events\":[\"e2\",\"e3\",\"e4\",\"e5\",\"e6\",\"e7\"]},{\"name\":\"stage-3\",\"events\":[\"e8\"]}]}"));
        expected.add(new ProducerRecord<>(OUTPUT_STREAM, K1,
                "{\"events\":[{\"name\":\"stage-1\",\"events\":[\"e3\"]},{\"name\":\"stage-2\",\"events\":[\"e4\",\"e6\"]},{\"name\":\"stage-3\",\"events\":[\"e8\"]}]}"));

        for (int i = 0; i < 4; i ++) {
            ProducerRecord<String, String> record = driver.readOutput(OUTPUT_STREAM, STRING_SERDE.deserializer(), STRING_SERDE.deserializer());
            Assert.assertEquals(expected.get(i).key(), record.key());
            Assert.assertEquals(expected.get(i).value(), record.value());

        }
    }
}
