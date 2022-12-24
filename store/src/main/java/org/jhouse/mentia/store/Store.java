package org.jhouse.mentia.store;


import org.jhouse.mentia.store.storage.StorageEngine;

public class Store {
    String name;
    StorageEngine storage;

    byte[] get(byte[] key) {
        return storage.get(key);
    }

    void put(byte[] key, byte[] val) {
        storage.put(key, val);
    }
}
