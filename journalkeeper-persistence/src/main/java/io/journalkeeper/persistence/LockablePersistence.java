package io.journalkeeper.persistence;

import java.io.IOException;

/**
 * @author LiYue
 * Date: 2020/3/18
 */
public interface LockablePersistence {
    void lock() throws IOException;
    void unlock() throws IOException;
}
