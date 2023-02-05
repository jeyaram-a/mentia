package org.jhouse.mentia.store.storage;

import org.jhouse.mentia.store.ByteArray;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JournalWriter implements Closeable {

    public static final String JOURNAL_PATH_FORMAT = "%s/%s-journal-%d";
    private BufferedOutputStream stream;

    private final ReadWriteLock journalLock = new ReentrantReadWriteLock();
    private int inBuffer;
    private int flushWatermark;
    private boolean async;

    private final JournalConfig config;

    private String journalPath;


    public JournalWriter(JournalConfig config) {
        this.config = config;
        try {
            if (config.async()) {
                this.async = true;
                if (config.flushWatermark() == null) {
                    throw new RuntimeException("Flush watermark cannot be null for async mod");
                }
                this.flushWatermark = config.flushWatermark();
            }
            journalPath = String.format(JOURNAL_PATH_FORMAT, config.basePath(), config.storeName(), config.id());
            stream = new BufferedOutputStream(new FileOutputStream(journalPath));
        } catch (FileNotFoundException ex) {
            throw new RuntimeException("Error while opening journal file" + journalPath);
        }
    }

    public void createNewJournalFile(int id) {
        try {
            this.stream.close();
            File currentJournal = new File(journalPath);
            if(!currentJournal.delete()) {
                throw new RuntimeException("Error in deleting journal old journal file");
            }
            journalPath = String.format(JOURNAL_PATH_FORMAT, config.basePath(), config.storeName(), id);
            stream = new BufferedOutputStream(new FileOutputStream(journalPath));
        } catch (IOException e) {
            throw new RuntimeException("Error while creating new journal file", e);
        }

    }


    // TODO insert assert and insert delimiter
    public void write(ByteArray keyArr, ByteArray valArr) {
        byte[] key = keyArr.get();
        byte[] val = valArr.get();

        int kLen = key.length;
        int vLen = val.length;
        int tot = Integer.BYTES + kLen + Integer.BYTES + vLen;
        Lock lock = null;

        try {
            var kLenBytes = ByteBuffer.allocate(4).putInt(kLen).array();
            var vLenBytes = ByteBuffer.allocate(4).putInt(vLen).array();
            lock = journalLock.writeLock();
            lock.lock();
            stream.write(kLenBytes);
            stream.write(key);
            stream.write(vLenBytes);
            stream.write(val);
            inBuffer += tot;
            if (async && inBuffer >= flushWatermark) {
                stream.flush();
            } else if (!async) {
                stream.flush();
            }
        } catch (Exception exception) {
            throw new WriteException(" ", exception);
        } finally {
            if(lock != null) {
                lock.unlock();
            }
        }
    }


    @Override
    public void close() throws IOException {
        stream.flush();
        stream.close();
    }
}
