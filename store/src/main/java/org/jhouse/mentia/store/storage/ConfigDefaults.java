package org.jhouse.mentia.store.storage;

public class ConfigDefaults {

    public static final int MB = 1 << 20;
    public static final int KB = 1 << 10;
    public static final int DEFAULT_JOURNAL_FLUSH_MARK = 100 * MB;
    public static final int DEFAULT_SEGMENT_INDEX_FOLD_MARK = 100 * MB;
}
