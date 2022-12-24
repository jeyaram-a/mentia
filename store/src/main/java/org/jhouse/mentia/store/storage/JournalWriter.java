package org.jhouse.mentia.store.storage;

import org.jhouse.mentia.store.ByteArray;

import java.io.*;
import java.nio.ByteBuffer;

public class JournalWriter implements Closeable {
    private final BufferedOutputStream stream;

    private int inBuffer;
    private int flushWatermark;
    private boolean async;

    public JournalWriter(JournalConfig config) {
        String journalPath = null;
        try {
            if (config.async()) {
                this.async = true;
                if (config.flushWatermark() == null) {
                    throw new RuntimeException("Flush watermark cannot be null for async mod");
                }
                this.flushWatermark = config.flushWatermark();
            }
            journalPath = String.format("%s-%d-journal", config.basePath(), config.id());
            System.out.println(journalPath);
            stream = new BufferedOutputStream(new FileOutputStream(journalPath));
        } catch (FileNotFoundException ex) {
            throw new RuntimeException("Error while opening journal file" + journalPath);
        }

    }


    // TODO insert assert and insert delimiter
    public void write(ByteArray keyArr, ByteArray valArr) {
        byte[] key = keyArr.get();
        byte[] val = valArr.get();

        int kLen = key.length;
        int vLen = val.length;
        int tot = Integer.BYTES + kLen + Integer.BYTES + vLen;

        try {
            var kLenBytes = ByteBuffer.allocate(4).putInt(kLen).array();
            var vLenBytes = ByteBuffer.allocate(4).putInt(vLen).array();
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
        } catch (IOException exception) {
            throw new WriteException("Error in writing to journal ", exception);
        }
    }


    @Override
    public void close() throws IOException {
        stream.flush();
        stream.close();
    }
}
