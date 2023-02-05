package org.jhouse.mentia.store.metadata;

import java.nio.channels.FileLock;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

public record StoreMetaData(FileLock lock, String storeRootPath, String name, int journalId, List<SegmentMetaData> compactedSegmentMetaDataList,
                            List<SegmentMetaData> nonCompactedSegmentMetaDataList, ReadWriteLock compactedSegmentMetaDataListLock, ReadWriteLock nonCompactedSegmentMetaDataListLock) {
}