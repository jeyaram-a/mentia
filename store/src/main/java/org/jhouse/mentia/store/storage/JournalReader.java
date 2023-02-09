package org.jhouse.mentia.store.storage;

import org.jhouse.mentia.store.ByteArray;
import org.tinylog.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;

public class JournalReader implements Cloneable {

    BufferedInputStream in;
    public JournalReader(String file) throws FileNotFoundException {
            in = new BufferedInputStream(new FileInputStream(file));

    }

    public TreeMap<ByteArray, ByteArray> read() {
        TreeMap<ByteArray, ByteArray> readEntries = new TreeMap<>();
        try {
            Logger.debug("Reading existing journal into segment in memory");
            while (true) {
                byte[] lenArr = new byte[4];
                int read = in.read(lenArr);
                if (read == -1) {
                    break;
                }
                int keyLen = ByteBuffer.wrap(lenArr).getInt();
                byte[] key = new byte[keyLen];
                read = in.read(key);
                if (read == -1) {
                    break;
                }
                read = in.read(lenArr);
                if (read == -1) {
                    break;
                }
                int valLen = ByteBuffer.wrap(lenArr).getInt();
                byte[] val = new byte[valLen];
                read = in.read(val);
                if (read == -1) {
                    break;
                }
                readEntries.put(new ByteArray(key), new ByteArray(val));
            }
            Logger.debug("Done reading existing journal into segment in memory");

            return readEntries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }




}
