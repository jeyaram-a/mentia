package org.jhouse.mentia.store.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class JournalReader implements Cloneable {

    BufferedInputStream in;
    public JournalReader(String file) {
        try {
            in = new BufferedInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public List<byte[][]> read() {
        List<byte[][]> readEntries = new LinkedList<>();
        try {
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
                byte[][] entry = new byte[2][];
                entry[0] = key;
                entry[1] = val;
                readEntries.add(entry);
            }
            return readEntries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }




}
