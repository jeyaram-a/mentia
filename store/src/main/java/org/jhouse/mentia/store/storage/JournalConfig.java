package org.jhouse.mentia.store.storage;

public record JournalConfig(String basePath, long id, boolean async, Integer flushWatermark) {
}
