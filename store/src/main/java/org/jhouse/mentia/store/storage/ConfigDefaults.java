package org.jhouse.mentia.store.storage;

public class ConfigDefaults {

    private static final int MB = 2 ^ 20;
    public static final int DEFAULT_JOURNAL_FLUSH_MARK = 100 * MB;
    public static final int DEFAULT_SEGMENT_INDEX_FOLD_MARK = 100 * MB;
}
