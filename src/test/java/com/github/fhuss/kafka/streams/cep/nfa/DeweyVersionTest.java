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
package com.github.fhuss.kafka.streams.cep.nfa;

import org.junit.Test;
import static org.junit.Assert.*;

public class DeweyVersionTest {

    @Test
    public void testDeweyVersionConstructor() {
        assertEquals("1", new DeweyVersion(1).toString());
    }

    @Test
    public void testStringConstructor() {
        assertEquals(new DeweyVersion("1.0.1").toString(), "1.0.1");
    }

    @Test
    public void testDeweyVersionToNewRun() {
        DeweyVersion version = new DeweyVersion(1).addRun();
        assertEquals(version.toString(), "2");
    }

    @Test
    public void testDeweyVersionToNewStageAndRun() {
        DeweyVersion version = new DeweyVersion(1)
                .addStage()
                .addRun();
        assertEquals(version.toString(), "1.1");
    }

    @Test
    public void testDeweyVersionToNewStage() {
        DeweyVersion version = new DeweyVersion(1).addStage();
        assertEquals(version.toString(), "1.0");
    }

    @Test
    public void testPredecessor() {
        assertFalse(new DeweyVersion("1.0").isCompatible(new DeweyVersion("2.0")));
        assertTrue(new DeweyVersion("1.0.0").isCompatible(new DeweyVersion("1.0")));
        assertTrue(new DeweyVersion("1.1").isCompatible(new DeweyVersion("1.0")));
        assertFalse(new DeweyVersion("1.0").isCompatible(new DeweyVersion("1.1")));
    }
}