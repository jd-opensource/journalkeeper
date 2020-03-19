/**
 * Copyright 2019 The JoyQueue Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.persistence.local.lock;

import io.journalkeeper.persistence.LockablePersistence;
import io.journalkeeper.persistence.PersistenceLockedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * from: https://gist.github.com/bmchae/1344404 by Burhan Uddin
 *
 * @author modified by liyue25
 * Date: 2018/10/10
 */
public class FileLock implements LockablePersistence {
    private static final Logger logger = LoggerFactory.getLogger(FileLock.class);
    private final File lockFile;
    private FileChannel channel;
    private java.nio.channels.FileLock lock;

    public FileLock(Path lockPath) {
        this.lockFile = lockPath.toFile();
    }

    public void lock() throws IOException {
        if(!lockFile.getParentFile().isDirectory() && !lockFile.getParentFile().mkdirs()) {
            throw new IOException(
                    String.format("Create directory %s failed!", lockFile.getParent())
            );
        }
        // Try to get the lock
        channel = new RandomAccessFile(lockFile, "rw").getChannel();

        lock = channel.tryLock();
        if (lock == null) {
            // File is lock by other application
            channel.close();

            throw new PersistenceLockedException(
                    String.format("Failed to acquire file lock: %s, file is locked by other process!", lockFile.getAbsolutePath())
            );
        }

    }

    public void unlock() {
        // release and delete file lock
        try {
            if (lock != null) {
                lock.release();
                channel.close();
                if(!lockFile.delete()) {
                    logger.warn("Delete lock file {} failed.", lockFile.getAbsolutePath());
                }
            }
        } catch (IOException e) {
            logger.warn("Unlock file {} failed!", lockFile.getAbsoluteFile());
        }
    }
}
