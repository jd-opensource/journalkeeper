package io.journalkeeper.base;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public interface Replicable {
    ReplicableIterator iterator();
}
