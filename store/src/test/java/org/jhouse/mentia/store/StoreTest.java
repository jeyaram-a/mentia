package org.jhouse.mentia.store;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.tinylog.Logger;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        config.setIndexJournalFlushWatermark(UtilConstants.MB * 10);
        config.setSegmentIndexFoldMark(UtilConstants.MB * 200);
        config.setCacheEnabled(true);
        config.setCacheSize(UtilConstants.MB * 5);
        var store = Store.open(path, "sample", config, pool);
        long tb = 0;
        int entrySize = 2000000;
        byte[][] keys = new byte[entrySize][];
        byte[][] vals = new byte[entrySize][];

        for (int i = 0; i < entrySize; ++i) {
            byte[] kb = ("hello-" + i).getBytes();
            byte[] vb = (("world-" + i)).repeat(100).getBytes();
            tb += kb.length + vb.length;
            keys[i] = kb;
            vals[i] = vb;
        }

        var start = System.currentTimeMillis();
        for(int i=0; i<entrySize; ++i) {
            store.put(keys[i], vals[i]);
        }

        var writeDuration = System.currentTimeMillis() - start;
        Logger.info(String.format("Writing took %d millis. Total %d bytes", writeDuration, tb));
        start = System.currentTimeMillis();
        var val = store.get("hello-1343113".getBytes());
        var readDuration = System.currentTimeMillis() - start;
        Logger.info("val returned "+new String(val));

        Logger.info(String.format("Read took %d millis", readDuration));
    }

    @Test
    @Order(2)
    void testExistingStoreOpen() {
        String path = "C:\\Users\\jeyar\\OneDrive\\Documents\\db\\sample";
        ExecutorService pool = Executors.newFixedThreadPool(10, Thread.ofVirtual().factory());
        var config = new StoreConfig();
        config.setAsyncWrite(true);
        config.setIndexJournalFlushWatermark(UtilConstants.MB);
        config.setSegmentIndexFoldMark(UtilConstants.MB * 2);
        config.setCacheEnabled(true);
        config.setCacheSize(UtilConstants.MB * 5);
        var store = Store.open(path, "sample", config, pool);
        var start = System.currentTimeMillis();
        var val = store.get("hello-1".getBytes());
        var readDuration = System.currentTimeMillis() - start;
        Logger.info("Returned "+new String(val));
        Logger.info(String.format("Read took %d millis", readDuration));
    }


}

