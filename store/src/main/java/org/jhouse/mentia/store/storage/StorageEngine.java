package org.jhouse.mentia.store.storage;


import org.jhouse.mentia.store.ByteArray;
import org.jhouse.mentia.store.StoreConfig;
import org.jhouse.mentia.store.metadata.SegmentMetaData;
import org.jhouse.mentia.store.metadata.StoreMetaData;
import org.jhouse.mentia.store.util.Instrumentation;
import org.tinylog.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageEngine implements Closeable {

    private final StoreMetaData storeMetadata;
    private final StoreConfig storeConfig;

    private int journalId = 1;
    private final JournalWriter journalWriter;

    private final ReadWriteLock currSegmentLock = new ReentrantReadWriteLock();
    private int currSegmentIndexSize = 0;
    private final TreeMap<ByteArray, ByteArray> currInMemorySegment = new TreeMap<>();

    private LRUCache cache;

    private final ExecutorService diskAccessPool;

    private void setup(StoreConfig config) {
        if (config.isAsyncWrite()) {
            if (config.getIndexJournalFlushWatermark() == 0) {
                config.setIndexJournalFlushWatermark(ConfigDefaults.DEFAULT_JOURNAL_FLUSH_MARK);
            }
        }

        if (config.getSegmentIndexFoldMark() == 0) {
            config.setSegmentIndexFoldMark(ConfigDefaults.DEFAULT_SEGMENT_INDEX_FOLD_MARK);
        }
    }

    public StorageEngine(StoreConfig storeConfig, StoreMetaData storeMetadata, ExecutorService diskAccessPool) {
        this.storeConfig = storeConfig;
        this.storeMetadata = storeMetadata;
        this.journalId = storeMetadata.journalId();
        setup(this.storeConfig);
        this.journalWriter = new JournalWriter(new JournalConfig(storeMetadata.storeRootPath(), storeMetadata.name(), journalId, storeConfig.isAsyncWrite(), storeConfig.getIndexJournalFlushWatermark()));
        this.diskAccessPool = diskAccessPool;
        if (storeConfig.isCacheEnabled()) {
            this.cache = new LRUCache(storeConfig.getCacheSize());
        }
    }

    private boolean isKeyInRange(byte[] key, byte[] min, byte[] max) {
        return Arrays.compare(min, key) <= 0 && Arrays.compare(key, max) <= 0;
    }

    private byte[] getFromMemory(byte[] key) {
        ByteArray val;
        var lock = currSegmentLock.readLock();
        lock.lock();
        val = currInMemorySegment.get(new ByteArray(key));
        lock.unlock();
        if (val == null) {
            return null;
        }
        return val.get();
    }


    public byte[] get(byte[] key) {

        var val = getFromMemory(key);

        if (val == null && cache != null) {
            ByteArray valFromCache = cache.get(new ByteArray(key));
            if (valFromCache != null) {
                return valFromCache.get();
            }
        }


        if (val == null) {
            {
                var nonCompactedSegments = storeMetadata.nonCompactedSegmentMetaDataList();
                var iterator = nonCompactedSegments.listIterator(nonCompactedSegments.size());
                while (iterator.hasPrevious()) {
                    var segment = iterator.previous();
                    if (isKeyInRange(key, segment.getHeader().min(), segment.getHeader().max())) {
                        int offset = Files.search(segment.getKeyIndexFile(), key, segment.getHeader().keyCount());
                        if (offset >= 0) {
                            val = Files.fetchVal(segment.getValFile(), offset, this.diskAccessPool);
                        }
                    }
                }
            }

            {
                var compactedSegments = storeMetadata.compactedSegmentMetaDataList();
                var iterator = compactedSegments.listIterator(compactedSegments.size());
                while (iterator.hasPrevious()) {
                    var segment = iterator.previous();
                    if (isKeyInRange(key, segment.getHeader().min(), segment.getHeader().max())) {
                        int offset = Files.search(segment.getKeyIndexFile(), key, segment.getHeader().keyCount());
                        if (offset >= 0) {
                            val = Files.fetchVal(segment.getValFile(), offset, this.diskAccessPool);
                        }
                    }
                }
            }

        }
        if(cache != null) {
            cache.put(new ByteArray(key), new ByteArray(val));
        }
        return val;
    }

    private void writeCurrSegmentAndReset() {
        var writeLock = currSegmentLock.writeLock();
        writeLock.lock();
        try {
            int prevId;
            var nonCompactedSegmentMetaDataList = storeMetadata.nonCompactedSegmentMetaDataList();
            if (nonCompactedSegmentMetaDataList.isEmpty()) {
                prevId = 0;
            } else {
                int lastIndex = storeMetadata.nonCompactedSegmentMetaDataList().size() - 1;
                prevId = lastIndex + 1;
            }
            int newId = prevId + 1;
            String keyFileName = storeMetadata.storeRootPath() + "/" + String.format(SegmentMetaData.NON_COMPACTED_KEY_FILE_NAME_FORMAT, storeMetadata.name(), newId);
            String valFileName = storeMetadata.storeRootPath() + "/" + String.format(SegmentMetaData.NON_COMPACTED_VAL_FILE_NAME_FORMAT, storeMetadata.name(), newId);
            String segmentHeaderFileName = storeMetadata.storeRootPath() + "/" + String.format(SegmentMetaData.NON_COMPACTED_SEGMENT_HEADER_FILE_NAME_FORMAT, storeMetadata.name(), newId);
            var newSegmentFile = new SegmentMetaData(keyFileName, segmentHeaderFileName, valFileName, newId, new SegmentMetaData.Header(currInMemorySegment.size(), currInMemorySegment.firstKey().get(), currInMemorySegment.lastKey().get()));
            this.storeMetadata.nonCompactedSegmentMetaDataList().add(newSegmentFile);
            Logger.info(String.format("%s segment file writing commencing. Id=%d", storeMetadata.name(), newSegmentFile.getId()));
            long timeTaken = Instrumentation.measure(() -> Files.writeSegment(newSegmentFile, this.currInMemorySegment, this.diskAccessPool));
            Logger.info(String.format("%s segment file written successfully. Took %d ms", storeMetadata.name(), timeTaken));
            this.currInMemorySegment.clear();
            this.currSegmentIndexSize = 0;
        } catch (Exception e) {
            throw new WriteException("Error in writing segment", e);
        } finally {
            writeLock.unlock();
        }

    }

    public void put(byte[] key, byte[] val) {
        Lock writeLock = null;
        try {
            var keyArr = new ByteArray(key);
            var valArr = new ByteArray(val);
            if (cache != null) {
                cache.put(keyArr, valArr);
            }
            journalWriter.write(keyArr, valArr);
            writeLock = currSegmentLock.writeLock();
            writeLock.lock();
            currInMemorySegment.put(keyArr, valArr);
            // Don't need to use atomic integer. Anyway under currSegmentLock.
            currSegmentIndexSize += key.length + val.length;
            if (currSegmentIndexSize >= storeConfig.getSegmentIndexFoldMark()) {
                writeCurrSegmentAndReset();
            }
        } catch (Exception ex) {
            Logger.error("Error in put operation ", ex);
            throw ex;
        } finally {
            if (writeLock != null) {
                writeLock.unlock();
            }
        }

    }

    @Override
    public void close() throws IOException {
        this.journalWriter.close();
    }
}
