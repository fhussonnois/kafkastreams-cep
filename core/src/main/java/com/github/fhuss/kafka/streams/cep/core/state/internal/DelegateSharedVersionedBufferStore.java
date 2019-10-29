package com.github.fhuss.kafka.streams.cep.core.state.internal;

import com.github.fhuss.kafka.streams.cep.core.Event;
import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.core.nfa.DeweyVersion;
import com.github.fhuss.kafka.streams.cep.core.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.core.state.SharedVersionedBufferStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * A shared version buffer implementation which delegate persistence of
 * match event to a {@link MatchedEventStore}.
 *
 * The implementation is based on the paper "Efficient Pattern Matching over Event Streams".
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class DelegateSharedVersionedBufferStore<K, V> implements SharedVersionedBufferStore<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(DelegateSharedVersionedBufferStore.class);

    private final MatchedEventStore<K, V> store;

    /**
     * Creates a new {@link DelegateSharedVersionedBufferStore} instance.
     *
     * @param store    the store used for delegating {@link MatchedEvent} persistence.
     */
    public DelegateSharedVersionedBufferStore(final MatchedEventStore<K, V> store)  {
        Objects.requireNonNull(store, "store cannot be null");
        this.store = store;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final Stage<K, V> currStage,
                    final Event<K, V> currEvent,
                    final Stage<K, V> prevStage,
                    final Event<K, V> prevEvent,
                    final DeweyVersion version) {

        Matched prevEventKey = Matched.from(prevStage, prevEvent);
        Matched currEventKey = Matched.from(currStage, currEvent);

        MatchedEvent<K, V> sharedPrevEvent = store.get(prevEventKey);

        if (sharedPrevEvent == null) {
            throw new IllegalStateException("Cannot find predecessor event for " + prevEventKey);
        }

        MatchedEvent<K, V> sharedCurrEvent = store.get(currEventKey);

        if (sharedCurrEvent == null) {
            sharedCurrEvent = new MatchedEvent<>(currEvent.key(), currEvent.value(), currEvent.timestamp());
        }
        sharedCurrEvent.addPredecessor(version, prevEventKey);
        LOG.debug("Putting event to store with key={}, value={}", currEventKey, sharedCurrEvent);
        store.put(currEventKey, sharedCurrEvent);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void branch(final Stage<K, V> stage, final Event<K, V> event, final DeweyVersion version) {
        Matched key = Matched.from(stage, event);
        MatchedEvent.Pointer pointer = new MatchedEvent.Pointer(version, key);
        while(pointer != null && (key = pointer.getKey()) != null) {
            final MatchedEvent<K, V> val = store.get(key);
            val.incrementRefAndGet();
            store.put(key, val);
            pointer = val.getPointerByVersion(pointer.getVersion());
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void put(final Stage<K, V> stage, final Event<K, V> event, final DeweyVersion version) {
        // A MatchedEvent can only by add once to a stack, so there is no need to check for existence.
        MatchedEvent<K, V> value = new MatchedEvent<>(event.key(), event.value(), event.timestamp());
        value.addPredecessor(version, null); // register an empty predecessor to kept track of the version (akka run).

        final Matched matched = new Matched(stage.getName(), stage.getType(), event.topic(), event.partition(), event.offset());
        LOG.debug("Putting event to store with key={}, value={}", matched, value);
        store.put(matched, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sequence<K, V> get(final Matched matched, final DeweyVersion version) {
        return peek(matched, version, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sequence<K, V> remove(final Matched matched, final DeweyVersion version) {
        return peek(matched, version, true);
    }

    private Sequence<K, V> peek(final Matched matched, DeweyVersion version, boolean remove) {
        MatchedEvent.Pointer pointer = new MatchedEvent.Pointer(version, matched);

        Sequence.Builder<K, V> builder = new Sequence.Builder<>();

        while (pointer != null && pointer.getKey() != null) {
            final Matched key = pointer.getKey();
            final MatchedEvent<K, V> stateValue = store.get(key);

            long refsLeft = stateValue.decrementRefAndGet();
            if (remove && refsLeft == 0 && stateValue.getPredecessors().size() <= 1) {
                store.delete(key);
            }

            builder.add(key.getStageName(), newEvent(key, stateValue));
            pointer = stateValue.getPointerByVersion(pointer.getVersion());

            if (remove && pointer != null && refsLeft == 0) {
                stateValue.removePredecessor(pointer);
                store.put(key, stateValue);

            }
        }
        return builder.build(true);
    }

    private Event<K, V> newEvent(final Matched stateKey, final MatchedEvent<K, V> stateValue) {
        return new Event<>(
            stateValue.getKey(),
            stateValue.getValue(),
            stateValue.getTimestamp(),
            stateKey.getTopic(),
            stateKey.getPartition(),
            stateKey.getOffset());
    }
}
