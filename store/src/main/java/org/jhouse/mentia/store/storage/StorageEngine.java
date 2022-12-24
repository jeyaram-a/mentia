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
    private StoreConfig storeConfig;

    private long journalId = 1L;
    private final JournalWriter journalWriter;

    private ReadWriteLock currSegmentLock = new ReentrantReadWriteLock();
    private int currSegmentIndexSize = 0;
    private TreeMap<ByteArray, ByteArray> currInMemorySegment = new TreeMap<>();
    private ExecutorService diskAccessPool;

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

    StorageEngine(StoreConfig storeConfig, StoreMetaData storeMetadata, ExecutorService diskAccessPool) {
        this.storeConfig = storeConfig;
        this.storeMetadata = storeMetadata;
        this.journalId = storeMetadata.journalId();
        setup(this.storeConfig);
        this.journalWriter = new JournalWriter(new JournalConfig(storeMetadata.storeRootPath(), journalId, storeConfig.isAsyncWrite(), storeConfig.getIndexJournalFlushWatermark()));
        this.diskAccessPool = diskAccessPool;
    }

    private boolean isKeyInRange(byte[] key, byte[] min, byte[] max) {
        return Arrays.compare(min, key) <= 0 && Arrays.compare(key, max) <= 0;
    }

    private byte[] getFromMemory(byte[] key) {
        ByteArray val = null;
        var lock = currSegmentLock.readLock();
        val = currInMemorySegment.get(key);
        lock.unlock();
        if (val == null) {
            return null;
        }
        return val.get();
    }


    public byte[] get(byte[] key) {

        var val = getFromMemory(key);

        if (val == null) {
            {
                var iterator = storeMetadata.nonCompactedSegmentMetaDataList().listIterator();
                while (iterator.hasPrevious()) {
                    var segment = iterator.previous();
                    if (isKeyInRange(key, segment.getMin(), segment.getMax())) {
                        int offset = Files.search(segment.getKeyIndexFile(), key, segment.getKeyCount());
                        if (offset >= 0) {
                            return Files.fetchVal(segment.getValFile(), offset, this.diskAccessPool);
                        }
                    }
                }
            }

            {
                var iterator = storeMetadata.compactedSegmentMetaDataList().listIterator();
                while (iterator.hasPrevious()) {
                    var segment = iterator.previous();
                    if (isKeyInRange(key, segment.getMin(), segment.getMax())) {
                        int offset = Files.search(segment.getKeyIndexFile(), key, segment.getKeyCount());
                        if (offset >= 0) {
                            return Files.fetchVal(segment.getValFile(), offset, this.diskAccessPool);
                        }
                    }
                }
            }

        }

        return null;
    }

    private void writeCurrSegmentAndReset() {
        var writeLock = currSegmentLock.writeLock();
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
            var newSegmentFile = new SegmentMetaData(keyFileName, valFileName, this.currInMemorySegment.size(),
                    currInMemorySegment.firstKey().get(), currInMemorySegment.lastKey().get(), newId);
            this.storeMetadata.nonCompactedSegmentMetaDataList().add(newSegmentFile);
            Logger.info(String.format("%s segment file writing commencing. Id=%d", storeMetadata.name(), newSegmentFile.getId()));
            long timeTaken = Instrumentation.measure(() -> {
                Files.writeSegment(newSegmentFile, this.currInMemorySegment, this.diskAccessPool);
            });
            Logger.info(String.format("%s segment file written successfully. Took %d ms", storeMetadata.name(), timeTaken));
        } catch (Exception e) {
            throw new  WriteException("Error in writing segment", e);
        } finally {
            writeLock.unlock();
        }

    }

    public void put(byte[] key, byte[] val) {
        Lock writeLock = null;
        try {
            var keyArr = new ByteArray(key);
            var valArr = new ByteArray(val);
            journalWriter.write(keyArr, valArr);
            writeLock = currSegmentLock.writeLock();
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
            if(writeLock != null) {
                writeLock.unlock();
            }
        }

    }

    @Override
    public void close() throws IOException {
        this.journalWriter.close();
    }
}
