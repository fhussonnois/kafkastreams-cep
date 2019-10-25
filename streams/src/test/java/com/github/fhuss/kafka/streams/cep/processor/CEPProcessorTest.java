package com.github.fhuss.kafka.streams.cep.processor;

import com.github.fhuss.kafka.streams.cep.core.nfa.Stages;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.core.pattern.PatternBuilder;
import com.github.fhuss.kafka.streams.cep.core.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.core.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.state.AggregatesStateStore;
import com.github.fhuss.kafka.streams.cep.state.NFAStateStore;
import com.github.fhuss.kafka.streams.cep.state.QueryStores;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStateStore;
import com.github.fhuss.kafka.streams.cep.state.internal.AggregatesStoreImpl;
import com.github.fhuss.kafka.streams.cep.state.internal.NFAStoreImpl;
import com.github.fhuss.kafka.streams.cep.state.internal.SharedVersionedBufferStoreImpl;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.RecordContextStub;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class CEPProcessorTest {

    private static final String TOPIC_TEST_1         = "topic-test-1";
    private static final String TOPIC_TEST_2         = "topic-test-2";
    private static final String DEFAULT_STRING_VALUE = "value";
    private static final String TEST_QUERY           = "test-query";

    private static final PatternBuilder<String, String> PATTERN =  new QueryBuilder<String, String>()
            .select()
            .where((event) -> true);

    private static final String KEY_1 = "key-1";
    private static final String KEY_2 = "key-2";

    private Pattern<String, String> pattern;
    private CEPProcessor<String, String> processor;
    private MockProcessorContext context;


    @Before
    public void before() {
        this.context = new MockProcessorContext();
        this.pattern = PATTERN.build();

        StagesFactory<String, String> factory = new StagesFactory<>();
        Stages<String, String> stages = factory.make(pattern);

        this.processor = new CEPProcessor<>(TEST_QUERY, stages);

        SharedVersionedBufferStateStore<String, String> bufferStore =  new SharedVersionedBufferStoreImpl<>(
            new InMemoryKeyValueStore<>(
                QueryStores.getQueryEventBufferStoreName(TEST_QUERY),
                Serdes.Bytes(),
                Serdes.ByteArray()
            ),
            Serdes.String(),
            Serdes.String()
        );

        AggregatesStateStore<String> aggStore = new AggregatesStoreImpl<>(
            new InMemoryKeyValueStore<>(
                QueryStores.getQueryAggregateStatesStoreName(TEST_QUERY),
                Serdes.Bytes(),
                Serdes.ByteArray()
            )
        );

        NFAStateStore<String, String> nfaStore = new NFAStoreImpl<>(
            new InMemoryKeyValueStore<>(
                QueryStores.getQueryNFAStoreName(TEST_QUERY),
                Serdes.Bytes(),
                Serdes.ByteArray()
            ),
            stages.getAllStages(),
            Serdes.String(),
            Serdes.String()
        );

        this.context.register(bufferStore);
        this.context.register(aggStore);
        this.context.register(nfaStore);

        bufferStore.init(this.context, null);
        aggStore.init(this.context, null);
        nfaStore.init(this.context, null);
    }

    @Test
    public void shouldNotProcessWhenKeyIsNull() {
        try{
            this.processor.process(null, DEFAULT_STRING_VALUE);
        }
        catch(Exception e){
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldNotProcessWhenValueIsNull() {
        try{
            this.processor.process(KEY_1, null);
        }
        catch(Exception e){
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldNotProcessEarliestRecordByTopic() {
        this.context.setRecordContext(new RecordContextStub(0, System.currentTimeMillis(), 0, TOPIC_TEST_1));
        this.processor.init(this.context);
        this.processor.process(KEY_1, DEFAULT_STRING_VALUE);

        this.context.setRecordContext(new RecordContextStub(0, System.currentTimeMillis(), 0, TOPIC_TEST_2));
        this.processor.process(KEY_2, DEFAULT_STRING_VALUE);

        this.context.setRecordContext(new RecordContextStub(0, System.currentTimeMillis(), 0, TOPIC_TEST_1));
        this.processor.process(KEY_1, DEFAULT_STRING_VALUE);

        this.context.setRecordContext(new RecordContextStub(0, System.currentTimeMillis(), 0, TOPIC_TEST_2));
        this.processor.process(KEY_2, DEFAULT_STRING_VALUE);

        Assert.assertEquals(2, this.context.forwardedValues.size());
        Assert.assertNotNull(this.context.forwardedValues.get(KEY_1));
        Assert.assertNotNull(this.context.forwardedValues.get(KEY_2));
    }

    public static class MockProcessorContext extends NoOpProcessorContext {

        private Map<String, StateStore> stores = new HashMap<>();

        void register(StateStore store) {
            this.stores.put(store.name(), store);
        }

        @Override
        public StateStore getStateStore(String name) {
            return stores.get(name);
        }
    }
}
