package org.jhouse.mentia.store.metadata;

public class SegmentMetaData {

    public static final String NON_COMPACTED_KEY_FILE_NAME_FORMAT = "%s-non-compacted-key-%d";
    public static final String NON_COMPACTED_VAL_FILE_NAME_FORMAT = "%s-non-compacted-val-%d";
    final private String keyIndexFile;
    final private String valFile;
    final private int keyCount;
    final private byte[] min;
    final private byte[] max;

    final private long id;

    private boolean compacted = false;

    public SegmentMetaData(String keyIndexFile, String valFile, int keyCount, byte[] min, byte[] max, long id) {
        this.keyIndexFile = keyIndexFile;
        this.valFile = valFile;
        this.keyCount = keyCount;
        this.id = id;
        this.min = min;
        this.max = max;
    }

    public String getKeyIndexFile() {
        return keyIndexFile;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public String getValFile() {
        return valFile;
    }

    public byte[] getMin() {
        return min;
    }

    public byte[] getMax() {
        return max;
    }

    public long getId() {
        return id;
    }

    public boolean isCompacted() {
        return compacted;
    }
}
