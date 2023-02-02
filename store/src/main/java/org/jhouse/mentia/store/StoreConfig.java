package org.jhouse.mentia.store;

public class StoreConfig {

    private boolean asyncWrite;

    private boolean cacheEnabled;
    private int indexJournalFlushWatermark, segmentIndexFoldMark;

    private long cacheSize;

    private int maxPendingCompactionCount;

    public boolean isAsyncWrite() {
        return asyncWrite;
    }

    public void setAsyncWrite(boolean asyncWrite) {
        this.asyncWrite = asyncWrite;
    }

    public int getIndexJournalFlushWatermark() {
        return indexJournalFlushWatermark;
    }

    public void setIndexJournalFlushWatermark(int indexJournalFlushWatermark) {
        this.indexJournalFlushWatermark = indexJournalFlushWatermark;
    }

    public int getSegmentIndexFoldMark() {
        return segmentIndexFoldMark;
    }

    public void setSegmentIndexFoldMark(int segmentIndexFoldMark) {
        this.segmentIndexFoldMark = segmentIndexFoldMark;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public int getMaxPendingCompactionCount() {
        return maxPendingCompactionCount;
    }

    public void setMaxPendingCompactionCount(int maxPendingCompactionCount) {
        this.maxPendingCompactionCount = maxPendingCompactionCount;
    }

    @Override
    public String toString() {
        return "StoreConfig{" +
                "asyncWrite=" + asyncWrite +
                ", cacheEnabled=" + cacheEnabled +
                ", indexJournalFlushWatermark=" + indexJournalFlushWatermark +
                ", segmentIndexFoldMark=" + segmentIndexFoldMark +
                ", cacheSize=" + cacheSize +
                '}';
    }
}
