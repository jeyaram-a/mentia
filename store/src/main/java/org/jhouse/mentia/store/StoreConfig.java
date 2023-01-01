package org.jhouse.mentia.store;

public class StoreConfig {

    private boolean asyncWrite;
    private int indexJournalFlushWatermark, segmentIndexFoldMark;

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
}
