package org.jhouse.mentia.store.storage;

import org.jhouse.mentia.store.ByteArray;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCache extends  LinkedHashMap<ByteArray, ByteArray> {
    private final long size;
    private long currSize;

    private Lock lock = new ReentrantLock();

    LRUCache(long size) {
        super( 100, 0.75F, true);
        this.size = size;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<ByteArray, ByteArray> eldest) {
        if(this.currSize > size) {
            return true;
        }
        return false;
    }

    @Override
    public ByteArray put(ByteArray key, ByteArray val) {
        try {
            lock.lock();
            this.currSize += key.get().length + val.get().length;
            return super.put(key, val);
        } finally {
            lock.unlock();
        }

    }
}
