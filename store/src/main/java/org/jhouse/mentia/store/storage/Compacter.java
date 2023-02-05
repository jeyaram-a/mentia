package org.jhouse.mentia.store.storage;

import org.jhouse.mentia.store.metadata.SegmentMetaData;
import org.jhouse.mentia.store.metadata.StoreMetaData;
import org.tinylog.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;

public class Compacter {

    record Entry(BufferedInputStream keyFile, BufferedInputStream valFile) {
    }

    byte[] readKey(BufferedInputStream keyFile) throws IOException {
        byte[] keyStructure = new byte[208];
        int read = keyFile.read(keyStructure);
        if (read == 0) {
            return null;
        }
        int keyL = keyStructure[0];
        return Arrays.copyOfRange(keyStructure, 1, keyL + 1);
    }

    byte[] readVal(BufferedInputStream valFile) throws IOException {
        byte[] len = new byte[4];
        int read = valFile.read(len);
        if (read == 0) {
            return null;
        }
        int intLen = ByteBuffer.wrap(len).getInt();
        return valFile.readNBytes(intLen);
    }

    void compact(StoreMetaData storeMetaData,
                 String keyFileName, String valFileName, String segmentHeaderFileName, int id) {

        TreeMap<byte[], Entry> keyEntryMap = new TreeMap<>(Arrays::compare);

        List<SegmentMetaData> nonCompactedSegmentMetaDataList = storeMetaData.nonCompactedSegmentMetaDataList();
        ReadWriteLock nonCompactedSegmentsListLock = storeMetaData.nonCompactedSegmentMetaDataListLock();
        List<SegmentMetaData> compactedSegmentMetaDataList = storeMetaData.compactedSegmentMetaDataList();
        ReadWriteLock compactedSegmentsListLock = storeMetaData.compactedSegmentMetaDataListLock();

        byte[] minKey = null, maxKey = null;
        try (var compactedKey = new BufferedOutputStream(new FileOutputStream(keyFileName));
             var compactedVal = new BufferedOutputStream(new FileOutputStream(valFileName));
             var compactedSegmentHeader = new BufferedOutputStream(new FileOutputStream(valFileName))) {

            var rLockNonCompacted = nonCompactedSegmentsListLock.readLock();
            rLockNonCompacted.lock();
            for (var mData : nonCompactedSegmentMetaDataList) {
                var entry = new Entry(new BufferedInputStream(new FileInputStream(mData.getKeyIndexFile())),
                        new BufferedInputStream(new FileInputStream(mData.getValFile())));
                keyEntryMap.put(readKey(entry.keyFile()), entry);
            }
            rLockNonCompacted.unlock();
            int totCount = 0;
            while (!keyEntryMap.isEmpty()) {
                ++totCount;
                Map.Entry<byte[], Entry> polled = keyEntryMap.pollFirstEntry();
                if (minKey == null) {
                    minKey = polled.getKey();
                }
                maxKey = polled.getKey();
                compactedKey.write(polled.getKey());
                compactedVal.write(readVal(polled.getValue().valFile()));
                byte[] nextKeyInPolledFile = readKey(polled.getValue().keyFile());
                if (nextKeyInPolledFile != null) {
                    keyEntryMap.put(nextKeyInPolledFile, polled.getValue());
                }
            }

            var wLockNonCompacted = nonCompactedSegmentsListLock.writeLock();
            wLockNonCompacted.lock();
            nonCompactedSegmentMetaDataList.clear();
            wLockNonCompacted.unlock();
            var header = new SegmentMetaData.Header(totCount, minKey, maxKey);
            compactedSegmentHeader.write(header.toBytes());
            var compactedSegmentMetaData = new SegmentMetaData(keyFileName, segmentHeaderFileName, valFileName, id, header);
            var wLockCompacted = compactedSegmentsListLock.writeLock();
            wLockCompacted.lock();
            compactedSegmentMetaDataList.add(compactedSegmentMetaData);
            wLockCompacted.unlock();
        } catch (Exception e) {
            Logger.error("Error in compaction ", e);
        }

    }
}
