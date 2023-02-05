package org.jhouse.mentia.store;


import org.jhouse.mentia.store.metadata.SegmentMetaData;
import org.jhouse.mentia.store.metadata.StoreMetaData;
import org.jhouse.mentia.store.storage.StorageEngine;
import org.tinylog.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Store {
    private final String name;
    private final StorageEngine storage;

    public Store(String name, StorageEngine storage) {
        this.name = name;
        this.storage = storage;
    }

    byte[] get(byte[] key) {
        return storage.get(key);
    }

    void put(byte[] key, byte[] val) {
        storage.put(key, val);
    }

    static SegmentMetaData.Header getSegmentHeader(File segmentKeyFile) throws IOException {
        try (var in = new FileInputStream(segmentKeyFile.getAbsoluteFile())) {
            int keyCount = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            byte minLen = (byte) in.read();
            byte[] min = in.readNBytes(minLen);
            int maxLen = (byte) in.read();
            byte[] max = in.readNBytes(maxLen);
            return new SegmentMetaData.Header(keyCount, min, max);
        }
    }

    static StoreMetaData getStoreMetaData(String storePath, String name) {
        var dir = new File(storePath);
        FileLock lock = null;
        if (!dir.exists()) {
            boolean creationStatus = dir.mkdirs();
            if (!creationStatus) {
                throw new RuntimeException("Cannot initialize store " + storePath);
            }
            String lockFilePath = dir.getAbsolutePath() + "/lock";
            try (FileChannel channel = FileChannel.open(Path.of(lockFilePath), new StandardOpenOption[]{StandardOpenOption.APPEND})) {
                lock = channel.tryLock();
            } catch (IOException e) {
                throw new RuntimeException("Cannot acquire lock on store. May be it is open in some other process", e);
            }
        }
        if (!dir.isDirectory()) {
            throw new RuntimeException("Invalid store path. Not a directory");
        }

        int maxJournalId = 1;

        List<SegmentMetaData> nonCompactedMetaData = new ArrayList<>();
        try {

            for (File file : Objects.requireNonNull(dir.listFiles())) {
                String journalPattern = "-journal-";
                String nonCompactedKeyPattern = "-non-compacted-key-";
                String fileName = file.getName();

                if (fileName.contains(journalPattern)) {
                    int index = fileName.lastIndexOf(journalPattern);
                    int offset = journalPattern.length();
                    int journalId = Integer.parseInt(file.getName().substring(index + offset, fileName.length()));
                    maxJournalId = Math.max(journalId, maxJournalId);
                } else if (fileName.contains(nonCompactedKeyPattern)) {
                    int index = fileName.lastIndexOf(nonCompactedKeyPattern);
                    int offset = nonCompactedKeyPattern.length();
                    int keyId = Integer.parseInt(file.getName().substring(index + offset, fileName.length()));
                    String keyFileName = file.getAbsolutePath();
                    String keySegmentHeaderFileName = file.getAbsolutePath().replace("non-compacted-key", "non-compacted-segment-header");
                    var header = getSegmentHeader(new File(keySegmentHeaderFileName));
                    String valFileName = file.getAbsolutePath().replace("non-compacted-key", "non-compacted-val");
                    if (!new File(valFileName).exists()) {
                        throw new RuntimeException("Value file not found " + valFileName);
                    }
                    nonCompactedMetaData.add(new SegmentMetaData(keyFileName, keySegmentHeaderFileName, valFileName, keyId, header));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error in init of store with path " + storePath, e);
        }
        nonCompactedMetaData.sort(Comparator.comparingInt(SegmentMetaData::getId));
        // TODO change compacted
        return new StoreMetaData(lock, storePath, name, maxJournalId, new ArrayList<>(), nonCompactedMetaData,
                new ReentrantReadWriteLock(), new ReentrantReadWriteLock());
    }

    public static Store open(String path, String name, StoreConfig config, ExecutorService diskAccessPool) {
        var storageEngine = new StorageEngine(config, getStoreMetaData(path, name), diskAccessPool);
        Logger.info(String.format("Opening %s. %s", name, config));
        return new Store(name, storageEngine);
    }

}
