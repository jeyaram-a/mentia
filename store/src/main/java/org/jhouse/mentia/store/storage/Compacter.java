package org.jhouse.mentia.store.storage;

import org.jhouse.mentia.store.metadata.SegmentMetaData;
import org.jhouse.mentia.store.metadata.StoreMetaData;
import org.tinylog.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

public class Compacter {

    record Entry(BufferedInputStream keyFile, BufferedInputStream valFile) {
    }

    byte[] readKey(BufferedInputStream keyFile) throws IOException {
        byte[] keyStructure = new byte[208];
        int read = keyFile.read(keyStructure);
        if (read == -1) {
            return null;
        }
        int keyL = keyStructure[0];
        return Arrays.copyOfRange(keyStructure, 1, keyL + 1);
    }

    byte[] readVal(BufferedInputStream valFile) throws IOException {
        byte[] len = new byte[4];
        int read = valFile.read(len);
        if (read == -1) {
            return null;
        }
        int intLen = ByteBuffer.wrap(len).getInt();
        return valFile.readNBytes(intLen);
    }

    void compact(StoreMetaData storeMetaData,
                 String keyFileName, String valFileName, String segmentHeaderFileName, int id) {


        List<String> toBeCompacted = storeMetaData.nonCompactedSegmentMetaDataList()
                .stream().map(SegmentMetaData::getKeyIndexFile).toList();
        Logger.debug("Commencing compaction. Compacting " + toBeCompacted);
        TreeMap<byte[], Entry> keyEntryMap = new TreeMap<>(Arrays::compare);

        List<SegmentMetaData> nonCompactedSegmentMetaDataList = storeMetaData.nonCompactedSegmentMetaDataList();
        ReadWriteLock nonCompactedSegmentsListLock = storeMetaData.nonCompactedSegmentMetaDataListLock();
        List<SegmentMetaData> compactedSegmentMetaDataList = storeMetaData.compactedSegmentMetaDataList();
        ReadWriteLock compactedSegmentsListLock = storeMetaData.compactedSegmentMetaDataListLock();

        byte[] minKey = null, maxKey = null;
        try (var compactedKey = new BufferedOutputStream(new FileOutputStream(keyFileName));
             var compactedVal = new BufferedOutputStream(new FileOutputStream(valFileName));
             var compactedSegmentHeader = new BufferedOutputStream(new FileOutputStream(segmentHeaderFileName))) {

            var rLockNonCompacted = nonCompactedSegmentsListLock.readLock();
            rLockNonCompacted.lock();
            for (var mData : nonCompactedSegmentMetaDataList) {
                var entry = new Entry(new BufferedInputStream(new FileInputStream(mData.getKeyIndexFile())),
                        new BufferedInputStream(new FileInputStream(mData.getValFile())));
                keyEntryMap.put(readKey(entry.keyFile()), entry);
            }
            rLockNonCompacted.unlock();
            int totCount = 0;
            int valOffSet = 0;
            while (!keyEntryMap.isEmpty()) {
                ++totCount;
                Map.Entry<byte[], Entry> polled = keyEntryMap.pollFirstEntry();
                if (minKey == null) {
                    minKey = polled.getKey();
                }
                maxKey = polled.getKey();
                byte[] keyContent = new byte[208];
                int keyL = polled.getKey().length;
                int keyOffset = 0;
                keyContent[keyOffset] = (byte) keyL;
                System.arraycopy(polled.getKey(), 0, keyContent, keyOffset+1, keyL);
                System.arraycopy(ByteBuffer.allocate(4).putInt(valOffSet).array(), 0, keyContent, keyOffset+204, 4);
                compactedKey.write(keyContent);

                byte[] val = readVal(polled.getValue().valFile());
                compactedVal.write(ByteBuffer.allocate(4).putInt(val.length).array());
                compactedVal.write(val);
                valOffSet += val.length + 4;
                byte[] nextKeyInPolledFile = readKey(polled.getValue().keyFile());
                if (nextKeyInPolledFile != null) {
                    keyEntryMap.put(nextKeyInPolledFile, polled.getValue());
                }
            }

            var wLockNonCompacted = nonCompactedSegmentsListLock.writeLock();
            List<SegmentMetaData> toBeDeleted = new ArrayList<>(nonCompactedSegmentMetaDataList);
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
            boolean deletedStatus = true;
            for (var segment : toBeDeleted) {
                Logger.debug("Compaction: deleting "+ segment.getKeyIndexFile());
                deletedStatus &= Files.deleteIfExists(Paths.get(segment.getKeyIndexFile()));
                Logger.debug("Compaction: deleting "+ segment.getSegmentHeaderFile());
                deletedStatus &= Files.deleteIfExists(Paths.get(segment.getSegmentHeaderFile()));
                Logger.debug("Compaction: deleting "+ segment.getValFile());
                deletedStatus &= Files.deleteIfExists(Paths.get(segment.getValFile()));
            }
            if(!deletedStatus) {
                throw  new RuntimeException("Cannot delete stale files");
            }
            Logger.debug("Compaction ended " + toBeCompacted);

        } catch (Exception e) {
            Logger.error("Error in compaction ", e);
        }

    }
}
