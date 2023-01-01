package org.jhouse.mentia.store.metadata;

public class SegmentMetaData {

    public static final String NON_COMPACTED_KEY_FILE_NAME_FORMAT = "%s-non-compacted-key-%d";
    public static final String NON_COMPACTED_VAL_FILE_NAME_FORMAT = "%s-non-compacted-val-%d";

    public record Header(int keyCount, byte[] min, byte[] max) {
    }
    final private Header header;
    final private String keyIndexFile;
    final private String valFile;
    final private int id;

    private boolean compacted = false;

    public SegmentMetaData(String keyIndexFile, String valFile, int id,  Header header) {
        this.keyIndexFile = keyIndexFile;
        this.valFile = valFile;
        this.header = header;
        this.id = id;
    }

    public String getKeyIndexFile() {
        return keyIndexFile;
    }

    public String getValFile() {
        return valFile;
    }

    public long getId() {
        return id;
    }

    public Header getHeader() {
        return header;
    }


}
