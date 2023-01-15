package org.jhouse.mentia.store;

import org.jhouse.mentia.store.storage.ConfigDefaults;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.tinylog.Logger;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class StoreTest {

    @Test
    @Order(1)
    void testStoreOpen() {
        String path = "C:\\Users\\jeyar\\OneDrive\\Documents\\db\\sample";
        File dir = new File(path);
        for (File file : dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        ExecutorService pool = Executors.newFixedThreadPool(10, Thread.ofVirtual().factory());
        var config = new StoreConfig();
        config.setAsyncWrite(true);
        config.setIndexJournalFlushWatermark(ConfigDefaults.MB * 10);
        config.setSegmentIndexFoldMark(ConfigDefaults.MB * 200);
        config.setCacheEnabled(true);
        config.setCacheSize(ConfigDefaults.MB * 10);
        var store = Store.open(path, "sample", config, pool);
        var start = System.currentTimeMillis();
        long tb = 0;
        for (int i = 0; i < 2000000; ++i) {
            byte[] kb = ("hello-" + i).getBytes();
            byte[] vb = (("world-" + i)).repeat(100).getBytes();
            tb += kb.length + vb.length;
            store.put(kb, vb);
        }
        var writeDuration = System.currentTimeMillis() - start;
        Logger.info(String.format("Writing took %d millis. Total %d bytes", writeDuration, tb));
        start = System.currentTimeMillis();
        var val = store.get("hello-134453".getBytes());
        var readDuration = System.currentTimeMillis() - start;
        Logger.info(String.format("Read took %d millis", readDuration));
    }

    @Test
    @Order(2)
    void testExistingStoreOpen() {
        String path = "C:\\Users\\jeyar\\OneDrive\\Documents\\db\\sample";
        ExecutorService pool = Executors.newFixedThreadPool(10, Thread.ofVirtual().factory());
        var config = new StoreConfig();
        config.setAsyncWrite(true);
        config.setIndexJournalFlushWatermark(ConfigDefaults.MB);
        config.setSegmentIndexFoldMark(ConfigDefaults.MB * 2);
        config.setCacheEnabled(true);
        config.setCacheSize(ConfigDefaults.MB * 10);
        var store = Store.open(path, "sample", config, pool);
        var start = System.currentTimeMillis();
        assertNotNull(store.get("hello-1".getBytes()));
        var readDuration = System.currentTimeMillis() - start;
        Logger.info(String.format("Read took %d millis", readDuration));
    }


}

