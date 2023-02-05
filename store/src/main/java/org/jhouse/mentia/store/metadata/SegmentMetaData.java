package org.jhouse.mentia.store.metadata;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SegmentMetaData {

    public static final String NON_COMPACTED_KEY_FILE_NAME_FORMAT = "%s-non-compacted-key-%d";
    public static final String NON_COMPACTED_VAL_FILE_NAME_FORMAT = "%s-non-compacted-val-%d";
    public static final String NON_COMPACTED_SEGMENT_HEADER_FILE_NAME_FORMAT = "%s-non-compacted-segment-header-%d";

    public static final String COMPACTED_KEY_FILE_NAME_FORMAT = "%s-compacted-key-%d";
    public static final String COMPACTED_VAL_FILE_NAME_FORMAT = "%s-compacted-val-%d";
    public static final String COMPACTED_SEGMENT_HEADER_FILE_NAME_FORMAT = "%s-compacted-segment-header-%d";

    public record Header(int keyCount, byte[] min, byte[] max) {
        public byte[] toBytes() {
            int offset = 0;
            byte[] bytes = new byte[Integer.BYTES + Byte.BYTES + min.length + Byte.BYTES + max.length];
            System.arraycopy(ByteBuffer.allocate(Integer.BYTES).putInt(keyCount).array(), 0, bytes, offset, Integer.BYTES);
            offset += 4;
            bytes[offset] = (byte) min.length;
            offset++;
            System.arraycopy(min, 0, bytes, offset, min.length);
            offset += min.length;
            bytes[offset] = (byte) max.length;
            offset++;
            System.arraycopy(max, 0, bytes, offset, max.length);
            return bytes;
        }

        @Override
        public String toString() {
            return "Header{" +
                    "keyCount=" + keyCount +
                    ", min=" + Arrays.toString(min) +
                    ", max=" + Arrays.toString(max) +
                    '}';
        }
    }
    final private Header header;
    final private String keyIndexFile;
    final private String valFile;

    final private String segmentHeaderFile;
    final private int id;

    private final boolean compacted = false;

    public SegmentMetaData(String keyIndexFile, String segmentHeaderFile, String valFile, int id,  Header header) {
        this.keyIndexFile = keyIndexFile;
        this.valFile = valFile;
        this.segmentHeaderFile = segmentHeaderFile;
        this.header = header;
        this.id = id;
    }

    public String getKeyIndexFile() {
        return keyIndexFile;
    }

    public String getValFile() {
        return valFile;
    }

    public int getId() {
        return id;
    }

    public Header getHeader() {
        return header;
    }

    public String getSegmentHeaderFile() {
        return segmentHeaderFile;
    }

    @Override
    public String toString() {
        return "SegmentMetaData{" +
                "header=" + header +
                ", keyIndexFile='" + keyIndexFile + '\'' +
                ", valFile='" + valFile + '\'' +
                ", segmentHeaderFile='" + segmentHeaderFile + '\'' +
                ", id=" + id +
                ", compacted=" + compacted +
                '}';
    }
}
