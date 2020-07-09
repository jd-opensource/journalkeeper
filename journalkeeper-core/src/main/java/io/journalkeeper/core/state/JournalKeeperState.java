/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.state;

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.entry.internal.ScalePartitionsEntry;
import io.journalkeeper.core.entry.internal.SetPreferredLeaderEntry;
import io.journalkeeper.core.journal.JournalSnapshot;
import io.journalkeeper.exceptions.StateRecoverException;
import io.journalkeeper.persistence.MetadataPersistence;
import io.journalkeeper.utils.files.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITIONS_START;

/**
 *
 * @author LiYue
 * Date: 2019/11/20
 */
public class JournalKeeperState implements Flushable {
    private static final Logger logger = LoggerFactory.getLogger(JournalKeeperState.class);
    static final String USER_STATE_PATH = "user";
    private static final String INTERNAL_STATE_PATH = "internal";
    private static final String INTERNAL_STATE_FILE = "state";
    private final Map<InternalEntryType, List<ApplyInternalEntryInterceptor>> internalEntryInterceptors = new HashMap<>();
    private final List<ApplyReservedEntryInterceptor> reservedEntryInterceptors = new CopyOnWriteArrayList<>();
    private final StateFactory userStateFactory;
    private final MetadataPersistence metadataPersistence;
    /**
     * State文件读写锁
     */
    private final ReadWriteLock stateFilesLock = new ReentrantReadWriteLock();
    /**
     * 状态读写锁
     * 读取状态是加乐观锁或读锁，变更状态时加写锁。
     */
    private final StampedLock stateLock = new StampedLock();
    Path path;
    private State userState;
    InternalState internalState;
    private Properties properties;

    public JournalKeeperState(StateFactory userStateFactory, MetadataPersistence metadataPersistence) {
        this.userStateFactory = userStateFactory;
        this.metadataPersistence = metadataPersistence;
    }

    public void init(Path path, List<URI> voters, Set<Integer> partitions, URI preferredLeader) throws IOException {
        Files.createDirectories(path.resolve(USER_STATE_PATH));
        InternalState internalState = new InternalState(new ConfigState(voters), partitions, preferredLeader);
        File lockFile = path.getParent().resolve(path.getFileName() + ".lock").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
             FileChannel fileChannel = raf.getChannel()) {
            FileLock lock = fileChannel.tryLock();
            if (null == lock) {
                throw new ConcurrentModificationException(
                        String.format(
                                "Some other thread is operating the state files! State: %s.", path.toString()
                        ));
            } else {
                flushInternalState(internalStateFile(path), internalState);
                lock.release();
            }
        } finally {
            lockFile.delete();
        }
    }

    public void flush() throws IOException {
        try {
            stateFilesLock.writeLock().lock();
            flushInternalState();
            flushUserState();
        } finally {
            stateFilesLock.writeLock().unlock();
        }
    }

    private void flushUserState(State userState) throws IOException {
        if (userState instanceof Flushable) {
            ((Flushable) userState).flush();
        }
    }

    private void flushUserState() throws IOException {
        flushUserState(userState);
    }

    void flushInternalState() throws IOException {
        flushInternalState(internalStateFile(path), internalState);
    }

    private Path internalStateFile(Path statePath) {
        return statePath.resolve(INTERNAL_STATE_PATH).resolve(INTERNAL_STATE_FILE);
    }

    private void flushInternalState(Path internalStateFile, InternalState internalState) throws IOException {
        metadataPersistence.save(internalStateFile, new PersistInternalState().fromInternalState(internalState));
    }

    public void recover(Path path, Properties properties) {
        recover(path, properties, false);
    }

    void recover(Path path, Properties properties, boolean internalStateOnly) {
        stateFilesLock.writeLock().lock();
        try {
            recoverUnsafe(path, properties, internalStateOnly);
        } finally {
            stateFilesLock.writeLock().unlock();
        }
    }

    private void recoverUnsafe(Path path, Properties properties, boolean internalStateOnly) {
        this.path = path;
        this.properties = properties;
        try {
            Files.createDirectories(path);
            this.internalState = recoverInternalState(internalStateFile(path));
            if (!internalStateOnly) {
                recoverUserStateUnsafe();
            }
        } catch (IOException e) {
            throw new StateRecoverException(e);
        }
    }


    private InternalState recoverInternalState(Path internalStateFile) throws IOException {
        return metadataPersistence.load(internalStateFile, PersistInternalState.class).toInternalState();
    }

    public synchronized void addInterceptor(InternalEntryType type, ApplyInternalEntryInterceptor internalEntryInterceptor) {
        List<ApplyInternalEntryInterceptor> list = this.internalEntryInterceptors.computeIfAbsent(type, key -> new LinkedList<>());
        list.add(internalEntryInterceptor);
    }

    public synchronized void removeInterceptor(InternalEntryType type, ApplyInternalEntryInterceptor internalEntryInterceptor) {
        List<ApplyInternalEntryInterceptor> list = internalEntryInterceptors.get(type);
        if (null != list) {
            list.remove(internalEntryInterceptor);
        }
    }

    public synchronized void addInterceptor(ApplyReservedEntryInterceptor interceptor) {
        reservedEntryInterceptors.add(interceptor);
    }

    public synchronized void removeInterceptor(ApplyReservedEntryInterceptor interceptor) {
        reservedEntryInterceptors.remove(interceptor);
    }

    public Path getPath() {
        return path;
    }

    public long lastIncludedIndex() {
        return internalState.getLastIncludedIndex();
    }

    public int lastIncludedTerm() {
        return internalState.getLastIncludedTerm();
    }

    /**
     * Next apply id
     **/
    public long lastApplied() {
        return lastIncludedIndex() + 1;
    }

    public StateResult applyEntry(JournalEntry entryHeader, EntryFuture entryFuture, RaftJournal journal) {
        int partition = entryHeader.getPartition();
        int batchSize = entryHeader.getBatchSize();

        StateResult result = new StateResult(null);
        long stamp = stateLock.writeLock();
        try {
            if (partition < RESERVED_PARTITIONS_START) {
                result = userState.execute(entryFuture, partition, lastApplied(), batchSize, journal);
            } else if (partition == INTERNAL_PARTITION) {
                applyInternalEntry(entryFuture.get());
            } else {

                for (ApplyReservedEntryInterceptor reservedEntryInterceptor : reservedEntryInterceptors) {
                    reservedEntryInterceptor.applyReservedEntry(entryHeader, entryFuture, lastApplied());
                }
            }
            internalState.setLastIncludedTerm(entryHeader.getTerm());
            internalState.next();
            result.setLastApplied(lastApplied());
        }
        finally {
            stateLock.unlockWrite(stamp);
        }
        return result;
    }


    private void applyInternalEntry(byte[] internalEntry) {
        InternalEntryType type = InternalEntriesSerializeSupport.parseEntryType(internalEntry);
        logger.info("Apply internal entry, type: {}", type);
        switch (type) {
            case TYPE_SCALE_PARTITIONS:
                internalState.setPartitions(InternalEntriesSerializeSupport.parse(internalEntry, ScalePartitionsEntry.class).getPartitions());
                break;
            case TYPE_SET_PREFERRED_LEADER:
                SetPreferredLeaderEntry setPreferredLeaderEntry = InternalEntriesSerializeSupport.parse(internalEntry);
                URI old = internalState.getPreferredLeader();
                internalState.setPreferredLeader(setPreferredLeaderEntry.getPreferredLeader());
                logger.info("Set preferred leader from {} to {}.", old, internalState.getPreferredLeader());
                break;
            default:
        }

        List<ApplyInternalEntryInterceptor> interceptors = internalEntryInterceptors.get(type);
        if (null != interceptors) {
            for (ApplyInternalEntryInterceptor interceptor : interceptors) {
                interceptor.applyInternalEntry(type, internalEntry);
            }
        }

        try {
            flushInternalState();
        } catch (IOException e) {
            logger.warn("Flush internal state exception! Path: {}.", path, e);
        }
    }


    void recoverUserState() throws IOException {
        long stamp = stateLock.writeLock();
        try {
            recoverUserStateUnsafe();
        } finally {
            stateLock.unlockWrite(stamp);
        }

    }

    public void recoverUserStateUnsafe() throws IOException {
        this.userState = userStateFactory.createState();
        Path userStatePath = path.resolve(USER_STATE_PATH);
        Files.createDirectories(userStatePath);
        userState.recover(path.resolve(USER_STATE_PATH), properties);

    }

    public StateQueryResult query(byte[] query, RaftJournal journal) {
        StateQueryResult result;
        long stamp = stateLock.tryOptimisticRead();
        result = new StateQueryResult(userState.query(query, journal), lastApplied());

        if (!stateLock.validate(stamp)) {
            stamp = stateLock.readLock();
            try {
                result = new StateQueryResult(userState.query(query, journal), lastApplied());
            } finally {
                stateLock.unlockRead(stamp);
            }
        }
        return result;
    }

    public void dump(Path destPath) throws IOException {
        flush();
        try {
            stateFilesLock.readLock().lock();

            FileUtils.dump(path, destPath);
        } finally {
            stateFilesLock.readLock().unlock();
        }
    }

    public List<URI> voters() {
        return internalState.getConfigState().voters();
    }

    public void close() {
        long stamp = stateLock.writeLock();
        try {
            closeUnsafe();
        } finally {
            stateLock.unlockWrite(stamp);
        }
    }

    public void closeUnsafe() {
        if (null != userState) {
            userState.close();
        }
    }

    public void clearUserState() throws IOException {
        FileUtils.deleteFolder(path.resolve(USER_STATE_PATH));
    }

    public void clear() throws IOException {
        FileUtils.deleteFolder(path);
    }

    public ConfigState getConfigState() {
        return internalState.getConfigState();
    }

    public void setConfigState(ConfigState configState) {
        internalState.setConfigState(configState);
    }

    public Set<Integer> getPartitions() {
        return internalState.getPartitions();
    }

    public URI getPreferredLeader() {
        return internalState.getPreferredLeader();
    }

    public JournalSnapshot getJournalSnapshot() {
        return internalState;
    }


    @Override
    public String toString() {
        return "JournalKeeperState{" +
                "internalState=" + internalState +
                ", path=" + path +
                '}';
    }
}
