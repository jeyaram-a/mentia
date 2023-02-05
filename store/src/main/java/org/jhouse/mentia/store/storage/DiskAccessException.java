package org.jhouse.mentia.store.storage;

public class DiskAccessException extends RuntimeException {
    public DiskAccessException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
