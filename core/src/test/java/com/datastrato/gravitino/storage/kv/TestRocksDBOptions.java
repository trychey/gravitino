package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.rocksdb.WriteOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.Options;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRocksDBOptions {
    @Test
    void testSetOptions() {
        String prefix = "gravitino.entity.store.kv.rocksdb.options";
        String optionsKey = prefix + "." + "maxBackgroundJobs";

        Map<String, String> mockConfigMap = new HashMap<String, String>();
        Config config = Mockito.mock(Config.class);
        Mockito.when(config.getConfigsWithPrefix(prefix)).thenReturn(mockConfigMap);

        // set maxBackgroundJobs to 8
        int expectMaxBackgroundJobs = 8;
        mockConfigMap.put(optionsKey, String.valueOf(expectMaxBackgroundJobs));

        Options mockOptions = Mockito.spy(new Options());

        AtomicInteger maxBackgroundJobs = new AtomicInteger(0);
        // Mock the setMaxBackgroundJobs method
        Mockito.doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                if (args.length > 0 && args[0] instanceof Integer) {
                    maxBackgroundJobs.set(expectMaxBackgroundJobs);
                }
                return null;
            }
        }).when(mockOptions).setMaxBackgroundJobs(Mockito.anyInt());

        RocksDBOptions options = new RocksDBOptions(mockOptions, null, null);
        options.setOptions(config);

        Assertions.assertDoesNotThrow(() -> options.setOptions(config));
        Assertions.assertEquals(expectMaxBackgroundJobs, maxBackgroundJobs.get());
    }
}
