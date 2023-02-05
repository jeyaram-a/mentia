package org.jhouse.mentia.store.storage;


import org.jhouse.mentia.store.ByteArray;
import org.jhouse.mentia.store.metadata.SegmentMetaData;
import org.tinylog.Logger;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class Files {

    public static int search(String fileName, byte[] key, int totKeysThisSegment) {
        int low = 0, high = totKeysThisSegment;
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
            while (low <= high) {
                int mid = (low + high) / 2;
                file.seek(mid * 208L);
                /*
                    key max size = 200 bytes
                    Since the max size of a segment is 1gb
                    key format -> byte+key+offset -> 4+200+4 bytes -> 208 bytes / key
                 */
                byte[] keyStructure = new byte[208];
                file.read(keyStructure);
                int keyL = keyStructure[0];
                byte[] midKey = Arrays.copyOfRange(keyStructure, 1, keyL + 1);
                if (Arrays.equals(key, midKey)) {
                    return ByteBuffer.wrap(Arrays.copyOfRange(keyStructure, 204, 208)).getInt();
                } else if (Arrays.compare(midKey, key) < 0) {
                    low = mid+1;
                } else {
                    high = mid - 1;
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("error in searching key in segment " + fileName, e);
        }
        return -1;
    }

    static byte[] fetchVal(String fileName, int offset, ExecutorService pool) {
        Future<byte[]> valFuture = pool.submit(() -> {
            try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
                file.seek(offset);
                int valLen = file.readInt();
                file.seek(offset + Integer.BYTES);
                byte[] val = new byte[valLen];
                file.read(val);
                return val;
            }
        });

        try {
            return valFuture.get();
        } catch (Exception e) {
            throw new DiskAccessException("Error in fetching val from " + fileName, e);
        }

    }

    public static void writeSegment(SegmentMetaData segmentMetaData, TreeMap<ByteArray, ByteArray> segmentInMemory, int valSize, ExecutorService pool) {
        Future<?> status = pool.submit(() -> {
            try (
                 var segmentHeaderFos = new FileOutputStream(segmentMetaData.getSegmentHeaderFile());
                 var segmentHeaderWriter = new BufferedOutputStream(segmentHeaderFos);
                 var rKFile = new RandomAccessFile(segmentMetaData.getKeyIndexFile(), "rw");
                 var rwKChannel = rKFile.getChannel();
                 var rVFile = new RandomAccessFile(segmentMetaData.getValFile(), "rw");
                 var rwVChannel = rVFile.getChannel()
            ) {
                Logger.info("Writing to "+ segmentMetaData.getKeyIndexFile());
                segmentHeaderWriter.write(segmentMetaData.getHeader().toBytes());
                int keyOffset = 0;
                int valOffset = 0;
                byte[] keyContent = new byte[segmentInMemory.size() * 208];
                byte[] valContent = new byte[segmentInMemory.size()*Integer.BYTES + valSize];
                for (Map.Entry<ByteArray, ByteArray> entry : segmentInMemory.entrySet()) {
                    byte[] key = entry.getKey().get();
                    byte[] val = entry.getValue().get();

                    keyContent[keyOffset] = (byte) key.length;
                    System.arraycopy(key, 0, keyContent, keyOffset+1, key.length);
                    System.arraycopy(ByteBuffer.allocate(4).putInt(valOffset).array(), 0, keyContent, keyOffset+204, 4);
                    keyOffset += 208;

                    System.arraycopy(ByteBuffer.allocate(Integer.BYTES).putInt(val.length).array(), 0, valContent, valOffset, Integer.BYTES);
                    System.arraycopy(val, 0, valContent, valOffset+4, val.length);
                    valOffset += 4 + val.length;
                }
                ByteBuffer wrKBuf = rwKChannel.map(FileChannel.MapMode.READ_WRITE, 0, keyContent.length);
                wrKBuf.put(keyContent);
                ByteBuffer wrVBuf = rwVChannel.map(FileChannel.MapMode.READ_WRITE, 0, valContent.length);
                wrVBuf.put(valContent);
            } catch (Exception e) {
                throw new WriteException("Error writing segmentInMemory to disk", e);
            }
        });
        try {
            status.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
