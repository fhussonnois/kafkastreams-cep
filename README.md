Complex Event Processing on top of KafkaStreams Processor API
====================================

###This repository is not stable - development is in progress.

## TODO / KNOWN ISSUES
 * NFA class is not tolerant to kafka rebalance operations.
 * A Stage state should be maintained per nfa run
 * NFA is not currently tolerant to at-least once semantic (keep a high water mark)
 
## How to build a CEP processor
```java
        Pattern<String, String> query = new SequenceQuery<String, String>()
                .select("first")
                    .where((key, value, timestamp, store) -> value.equals("A"))
                .followBy("second")
                    .where((key, value, timestamp, store) -> value.equals("B"))
                .followBy("three")
                    .where((key, value, timestamp, store) -> value.equals("C"))
                    .withStrategy(Pattern.SelectStrategy.SKIP_TIL_ANY_MATCH)
                .followBy("latest")
                    .where((key, value, timestamp, store) -> value.equals("D"))
                    .withStrategy(Pattern.SelectStrategy.SKIP_TIL_ANY_MATCH);

        
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("MyEventSource", "MyTopic")
                .addProcessor("cep-proc", () -> new CEPProcessor<>(query), "MyEventSource");

        topologyBuilder.addStateStore(CEPProcessor.getEventsStore(true), "cep-proc"); // required

        //Use the topologyBuilder and streamingConfig to start the kafka streams process
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, streamingConfig);
        streaming.start();
```
## Support for event selection strategies
 * Strict contiguity
 * Skip till next match
 * Skip till any match
 
###Licence
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License