package io.journalkeeper.core.state;

import io.journalkeeper.core.entry.reserved.ReservedEntryType;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public interface ApplyInternalEntryInterceptor {
    void applyInternalEntry(ReservedEntryType type, byte [] internalEntry);
}
