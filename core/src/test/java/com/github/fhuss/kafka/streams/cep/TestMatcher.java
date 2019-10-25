package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.core.pattern.SimpleMatcher;

public class TestMatcher {

    public static <K>  SimpleMatcher<K, String> isEqualTo(final String value) {
        return event -> event.value().equals(value);
    }

    public static <K>  SimpleMatcher<K, Integer> isEqualTo(final Integer value) {
        return event -> event.value().equals(value);
    }

    public static <K>  SimpleMatcher<K, Integer> isGreaterThan(final Integer value) {
        return event -> event.value() > value;
    }

    public static <K>  SimpleMatcher<K, Integer> isLessThan(final Integer value) {
        return event -> event.value() > value;
    }
}
