package org.jhouse.mentia.store.metadata;

import java.util.List;

public record StoreMetaData(String storeRootPath, String name, int journalId, List<SegmentMetaData> compactedSegmentMetaDataList,
                            List<SegmentMetaData> nonCompactedSegmentMetaDataList) {
}