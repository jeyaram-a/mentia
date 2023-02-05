package org.jhouse.mentia.store.storage;

public class WriteException extends RuntimeException {
    public WriteException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
