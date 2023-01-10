package org.jhouse.mentia.store;


import org.jhouse.mentia.store.metadata.SegmentMetaData;
import org.jhouse.mentia.store.metadata.StoreMetaData;
import org.jhouse.mentia.store.storage.StorageEngine;
import org.tinylog.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;

public class Store {
    private String name;
    private StorageEngine storage;

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
        try(var in = new FileInputStream(segmentKeyFile.getAbsoluteFile())) {
            int keyCount = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            byte minLen = (byte)in.read();
            byte[] min = in.readNBytes(minLen);
            int maxLen = (byte) in.read();
            byte[] max = in.readNBytes(maxLen);
            return new SegmentMetaData.Header(keyCount, min, max);
        }
    }

    static StoreMetaData getStoreMetaData(String storePath, String name) {
        var dir = new File(storePath);
        if (!dir.exists()) {
            boolean creationStatus = dir.mkdirs();
            if(!creationStatus) {
                throw new RuntimeException("Cannot initialize store "+ storePath);
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
            throw new RuntimeException("Error in init of store with path "+storePath, e);
        }
        nonCompactedMetaData.sort(Comparator.comparingInt(SegmentMetaData::getId));
        return new StoreMetaData(storePath, name, maxJournalId, new ArrayList<>(), nonCompactedMetaData);
    }

    public static Store open(String path, String name, StoreConfig config, ExecutorService diskAccessPool) {
        var storageEngine = new StorageEngine(config, getStoreMetaData(path, name), diskAccessPool);
        Logger.info(String.format("Opening %s. %s", name, config));
        return new Store(name, storageEngine);
    }

    void close() {

    }
}
