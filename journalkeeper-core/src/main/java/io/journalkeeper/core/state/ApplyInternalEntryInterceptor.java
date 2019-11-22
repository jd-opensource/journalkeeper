package io.journalkeeper.core.state;

import io.journalkeeper.core.entry.internal.InternalEntryType;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public interface ApplyInternalEntryInterceptor {
    void applyInternalEntry(InternalEntryType type, byte [] internalEntry);
}
