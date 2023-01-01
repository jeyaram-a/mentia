package org.jhouse.mentia.store.storage;


import org.jhouse.mentia.store.ByteArray;
import org.jhouse.mentia.store.metadata.SegmentMetaData;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
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
                file.seek(mid * 236L);
                /*
                    key max size = 200 bytes
                    Since the max size of a segment is 1gb
                    key format -> byte+key+offset -> 4+200+32 bytes -> 236 bytes / key
                 */
                byte[] keyStructure = new byte[236];
                file.read(keyStructure);
                int keyL = keyStructure[0];
                byte[] midKey = Arrays.copyOfRange(keyStructure, 1, keyL + 1);
                if (Arrays.equals(key, midKey)) {
                    return ByteBuffer.wrap(Arrays.copyOfRange(keyStructure, keyL, 237)).getInt();
                } else if (Arrays.compare(midKey, key) < 0) {
                    low = mid + 1;
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
                file.seek(offset + Long.BYTES);
                byte[] val = new byte[valLen];
                file.read(val);
                return val;
            }
        });

        try {
            return valFuture.get();
        } catch (Exception e) {
            throw new DiskAccessException("Error in fetching val from "+fileName, e);
        }

    }

    public static void writeSegment(SegmentMetaData segmentMetaData, TreeMap<ByteArray, ByteArray> segmentInMemory, ExecutorService pool) {
        Future status = pool.submit(() -> {
            BufferedOutputStream keyFileWriter = null, valFileWriter = null;
            FileLock keyIndexFLock = null, valFLock = null;
            try {
                var keyIndexFos = new FileOutputStream(segmentMetaData.getKeyIndexFile());
                var valFos = new FileOutputStream(segmentMetaData.getValFile());

                keyIndexFLock = keyIndexFos.getChannel().lock();
                valFLock = valFos.getChannel().lock();

                keyFileWriter = new BufferedOutputStream(keyIndexFos);
                valFileWriter = new BufferedOutputStream(valFos);
                int valOffset = 0;
                for (Map.Entry<ByteArray, ByteArray> entry : segmentInMemory.entrySet()) {
                    byte[] key = entry.getKey().get();
                    byte[] val = entry.getValue().get();

                    byte[] keyInFormat = new byte[236];

                    keyInFormat[0] = (byte) key.length;
                    System.arraycopy(key, 0, keyInFormat, 1, key.length);
                    System.arraycopy(ByteBuffer.allocate(4).putInt(valOffset).array(), 0, keyInFormat, 204, 4);

                    byte[] valInFormat = new byte[Integer.BYTES + val.length];
                    System.arraycopy(ByteBuffer.allocate(Integer.BYTES).putInt(val.length).array(), 0, valInFormat, 0, Integer.BYTES);
                    System.arraycopy(val, 0, valInFormat, 4, val.length);
                    valOffset += valInFormat.length;

                    keyFileWriter.write(keyInFormat);
                    valFileWriter.write(valInFormat);
                }
                keyFileWriter.flush();
                valFileWriter.flush();
            } catch (Exception e) {
                throw  new WriteException("Error writing segmentInMemory to disk", e);
            } finally {
                try {
                    if (keyIndexFLock != null) {
                        keyIndexFLock.release();
                    }
                    if (valFLock != null) {
                        valFLock.release();
                    }

                    if (keyFileWriter != null) {
                        keyFileWriter.close();
                    }

                    if (valFileWriter != null) {
                        valFileWriter.close();
                    }

                } catch (Exception e) {
                    throw new WriteException("Error writing segmentInMemory to disk in finally block ", e);
                }

            }
        });
        try {
            status.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
