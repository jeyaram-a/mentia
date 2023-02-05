package org.jhouse.mentia.store.storage;

public record JournalConfig(String basePath, String storeName, long id, boolean async, Integer flushWatermark) {
}
