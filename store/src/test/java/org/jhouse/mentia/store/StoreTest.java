package org.jhouse.mentia.store;

import org.jhouse.mentia.store.storage.ConfigDefaults;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.*;

class StoreTest {

    @Test
    void testStoreOpen() {
        String path = "C:\\Users\\jeyar\\OneDrive\\Documents\\db\\sample";
        ExecutorService pool = Executors.newFixedThreadPool(10, Thread.ofVirtual().factory());
        var config = new StoreConfig();
        config.setIndexJournalFlushWatermark(ConfigDefaults.KB*10);
        var store = Store.open(path, "sample", config, pool);
        for(int i=0; i<200; ++i) {
            store.put(("hello-"+i).getBytes(), ("world-"+i).getBytes());
        }
        System.out.println(new String(store.get("hello-3".getBytes())));

    }

}