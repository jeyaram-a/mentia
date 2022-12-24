package org.jhouse.mentia.store.util;

public class Instrumentation {
    public static long measure(Runnable task) {
        long start = System.currentTimeMillis();
        task.run();
        return System.currentTimeMillis() - start;
    }
}
