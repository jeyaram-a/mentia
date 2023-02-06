package org.jhouse.mentia.store;

import org.jhouse.mentia.store.util.Instrumentation;
import org.junit.jupiter.api.Assertions;
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
        String path = "/home/j/db/sample";
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
        config.setCompressionEnabled(true);
        var store = Store.open(path, "sample", config, pool);
        long tb = 0;
        int entrySize = 2000000;
        byte[][] keys = new byte[entrySize][];
        byte[][] vals = new byte[entrySize][];
        Logger.info("preparing input");
        for (int i = 0; i < entrySize; ++i) {
            byte[] kb = ("hello-" + i).getBytes();
            byte[] vb = (("world-" + i)).repeat(100).getBytes();
            tb += kb.length + vb.length;
            keys[i] = kb;
            vals[i] = vb;
        }
        Logger.info("done preparing input");
        long writeDuration = Instrumentation.measure(() -> {
            for(int i=0; i<entrySize; ++i) {
//                Logger.info("Storing "+i);
                store.put(keys[i], vals[i]);
            }
        }) ;

        Logger.info(String.format("Writing took %d millis. Total %d bytes", writeDuration, tb));

        long readDuration = Instrumentation.measure(() -> {
            var val = store.get("hello-1".getBytes());
            Logger.info("val returned "+new String(val));

        });

        Logger.info(String.format("Read took %d millis", readDuration));
        store.shutDown();
    }

    @Test
    @Order(2)
    void testExistingStoreOpen() {
        String path = "/home/j/db/sample";
        ExecutorService pool = Executors.newFixedThreadPool(10, Thread.ofVirtual().factory());
        var config = new StoreConfig();
        config.setAsyncWrite(true);
        config.setIndexJournalFlushWatermark(UtilConstants.MB * 10);
        config.setSegmentIndexFoldMark(UtilConstants.MB * 200);
        config.setCacheEnabled(true);
        config.setCacheSize(UtilConstants.MB * 5);
        config.setCompressionEnabled(true);
        var store = Store.open(path, "sample", config, pool);
        var readDuration = Instrumentation.measure(() -> {
            var val = store.get("hello-1".getBytes());
            Logger.info("Val is "+ new String(val));
        });
        Logger.info(String.format("Read took %d millis", readDuration));
        readDuration = Instrumentation.measure(() -> {
            var val = store.get("hello-8".getBytes());
            Logger.info("Val is "+ new String(val));
        });
        Logger.info(String.format("Read took %d millis", readDuration));


    }


}

