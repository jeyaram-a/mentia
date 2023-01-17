package org.jhouse.mentia.store.storage;

import static org.jhouse.mentia.store.UtilConstants.MB;
public class ConfigDefaults {
    public static final int DEFAULT_JOURNAL_FLUSH_MARK = 100 * MB;
    public static final int DEFAULT_SEGMENT_INDEX_FOLD_MARK = 100 * MB;
}
