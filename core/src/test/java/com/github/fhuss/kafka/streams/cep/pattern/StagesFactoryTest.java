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
package com.github.fhuss.kafka.streams.cep.pattern;

import com.github.fhuss.kafka.streams.cep.TestMatcher;
import com.github.fhuss.kafka.streams.cep.nfa.EdgeOperation;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.nfa.Stages;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StagesFactoryTest {

    private static final String STAGE_1  = "stage-1";
    private static final String STAGE_2  = "stage-2";
    private static final String STAGE_3  = "stage-3";


    @Test(expected = StagesFactory.InvalidPatternException.class)
    public void testInvalidPattern() {

        Pattern<Object, String> pattern = new QueryBuilder<Object, String>()
                .select()
                .oneOrMore()
                .where(TestMatcher.isEqualTo("N/A"))
                .build();
        StagesFactory<Object, String> factory = new StagesFactory<>();
        factory.make(pattern);
    }

    @Test
    public void testPatternWithSingleStage() {
        Pattern<String, Integer> pattern = new QueryBuilder<String, Integer>()
                .select(STAGE_1).where(TestMatcher.isEqualTo(0)).build();

        StagesFactory<String, Integer> factory = new StagesFactory<>();
        Stages<String, Integer> stages = factory.make(pattern);

        Assert.assertEquals(2, stages.getAllStages().size());
        final Stage<String, Integer> stage0 = stages.getAllStages().get(0);

        Assert.assertEquals(Stage.StateType.FINAL, stage0.getType());
        Assert.assertEquals(0, stage0.getEdges().size());

        final Stage<String, Integer> stage1 = stages.getAllStages().get(1);

        Assert.assertEquals(Stage.StateType.BEGIN, stage1.getType());
        Assert.assertEquals(1, stage1.getEdges().size());
        Assert.assertEquals(true, stage1.getEdges().get(0).is(EdgeOperation.BEGIN));
        Assert.assertEquals(stage0, stage1.getEdges().get(0).getTarget());
        Assert.assertEquals(STAGE_1, stage1.getName());
    }

    @Test
    public void testPatternWithMultipleStages() {
        Pattern<String, Integer> pattern = new QueryBuilder<String, Integer>()
                .select(STAGE_1).where(TestMatcher.isEqualTo(0))
                .then()
                .select(STAGE_2).where((event) -> event.value() % 2 == 0)
                .then()
                .select(STAGE_3).where(TestMatcher.isGreaterThan(100))
                .build();

        StagesFactory<String, Integer> factory = new StagesFactory<>();
        Stages<String, Integer> patternStages = factory.make(pattern);

        List<Stage<String, Integer>> stages = patternStages.getAllStages();

        Assert.assertEquals(4, stages.size());
        Assert.assertEquals(Stage.StateType.FINAL, stages.get(0).getType());

        Assert.assertEquals(Stage.StateType.NORMAL, stages.get(1).getType());
        Assert.assertEquals(STAGE_3, stages.get(1).getName());

        Assert.assertEquals(Stage.StateType.NORMAL, stages.get(2).getType());
        Assert.assertEquals(STAGE_2, stages.get(2).getName());

        Assert.assertEquals(Stage.StateType.BEGIN, stages.get(3).getType());
        Assert.assertEquals(STAGE_1, stages.get(3).getName());
    }

    @Test
    public void testPatternWithMultipleStagesAndOneOrMore() {
        Pattern<String, Integer> pattern = new QueryBuilder<String, Integer>()
                .select(STAGE_1).where((event) -> event.value() == 0)
                .then()
                .select(STAGE_2).oneOrMore().where((event) -> event.value() % 2 == 0)
                .then()
                .select(STAGE_3).where((event) -> event.value() > 100)
                .build();

        StagesFactory<String, Integer> factory = new StagesFactory<>();
        Stages<String, Integer> patternStages = factory.make(pattern);
        List<Stage<String, Integer>> stages = patternStages.getAllStages();

        Assert.assertEquals(5, stages.size());

        // stage FINAL
        final Stage<String, Integer> stage0 = stages.get(0);
        Assert.assertEquals(Stage.StateType.FINAL, stage0.getType());

        // stage 3
        final Stage<String, Integer> stage3 = stages.get(1);
        Assert.assertEquals(Stage.StateType.NORMAL, stage3.getType());
        Assert.assertEquals(STAGE_3, stage3.getName());
        Assert.assertEquals(EdgeOperation.BEGIN, stage3.getEdges().get(0).getOperation());
        Assert.assertEquals(stage0.getName(), stage3.getEdges().get(0).getTarget().getName());

        // stage 2
        final Stage<String, Integer> stage2 = stages.get(2);
        Assert.assertEquals(Stage.StateType.NORMAL, stage2.getType());
        Assert.assertEquals(STAGE_2, stage2.getName());
        Assert.assertEquals(EdgeOperation.TAKE, stage2.getEdges().get(0).getOperation());
        Assert.assertEquals(stage3.getName(), stage2.getEdges().get(0).getTarget().getName());

        Assert.assertEquals(EdgeOperation.PROCEED, stage2.getEdges().get(1).getOperation());
        Assert.assertEquals(stage3.getName(), stage2.getEdges().get(1).getTarget().getName());

        // stage 2 (internal)
        final Stage<String, Integer> internalStage2 = stages.get(3);
        Assert.assertEquals(Stage.StateType.NORMAL, internalStage2.getType());
        Assert.assertEquals(STAGE_2, internalStage2.getName());
        Assert.assertEquals(EdgeOperation.BEGIN, internalStage2.getEdges().get(0).getOperation());

        // stage BEGIN
        final Stage<String, Integer> stage1 = stages.get(4);
        Assert.assertEquals(Stage.StateType.BEGIN, stage1.getType());
        Assert.assertEquals(STAGE_1, stage1.getName());
    }
}
