package com.github.fhuz.kafka.streams.cep.nfa;

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