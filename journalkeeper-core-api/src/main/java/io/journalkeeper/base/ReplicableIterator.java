package io.journalkeeper.base;

import java.io.IOException;

/**
 * @author LiYue
 * Date: 2019/11/21
 */
public interface ReplicableIterator {
    byte [] nextTrunk() throws IOException;
    boolean hasMoreTrunks();
}
