package com.github.fhuss.kafka.streams.cep.pattern;

import com.github.fhuss.kafka.streams.cep.Event;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.state.ReadOnlySharedVersionBuffer;
import com.github.fhuss.kafka.streams.cep.state.States;
import com.github.fhuss.kafka.streams.cep.state.internal.Matched;

/**
 * A matcher defines the condition under which an event should be selected to be added to the pattern sequence.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
@FunctionalInterface
public interface SequenceMatcher<K, V> extends Matcher<K, V> {

    @Override
    default boolean accept(final MatcherContext<K, V> context) {
        ReadOnlySharedVersionBuffer<K, V> buffer = context.getBuffer();
        Sequence<K, V> sequence = buffer.get(Matched.from(context.getPreviousStage(), context.getPreviousEvent()), context.getVersion());
        return matches(context.getCurrentEvent(), sequence, context.getStates());
    }

    /**
     * The function that evaluates an input record stream.
     *
     * @param event         the current event in the stream.
     * @param sequence      the previous accepted events.
     * @param states        the states store for this pattern.
     *
     * @return <code>true</code> if the event should be selected.
     */
    boolean matches(final Event<K, V> event, final Sequence<K, V> sequence, final States states);
}
