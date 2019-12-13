/**
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

import io.journalkeeper.base.Replicable;
import io.journalkeeper.base.ReplicableIterator;
import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.entry.internal.ScalePartitionsEntry;
import io.journalkeeper.core.entry.internal.SetPreferredLeaderEntry;
import io.journalkeeper.core.exception.StateRecoverException;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.core.journal.JournalSnapshot;
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
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
public class JournalKeeperState <E, ER, Q, QR> implements Replicable, Flushable {
    private static final Logger logger = LoggerFactory.getLogger(JournalKeeperState.class);
    private static final String USER_STATE_PATH = "user";
    private static final String INTERNAL_STATE_PATH = "internal";
    private static final String INTERNAL_STATE_FILE = "state";
    private static final int MAX_TRUNK_SIZE = 1024 * 1024;
    private Path path;
    private State<E, ER, Q, QR> userState;
    private InternalState internalState;
    private final Map<InternalEntryType, List<ApplyInternalEntryInterceptor>> internalEntryInterceptors = new HashMap<>();
    private final List<ApplyReservedEntryInterceptor> reservedEntryInterceptors = new CopyOnWriteArrayList<>();
    private final StateFactory<E, ER, Q, QR> userStateFactory;
    private final MetadataPersistence metadataPersistence;
    private Properties properties;
    /**
     * State文件读写锁
     */
    private final ReadWriteLock stateFilesLock = new ReentrantReadWriteLock();

    /**
     * 状态读写锁
     * 读取状态是加乐观锁或读锁，变更状态时加写锁。
     */
    private final StampedLock stateLock = new StampedLock();

    public JournalKeeperState(StateFactory<E, ER, Q, QR> userStateFactory, MetadataPersistence metadataPersistence) {
        this.userStateFactory = userStateFactory;
        this.metadataPersistence = metadataPersistence;
    }

    public void init(Path path,  List<URI> voters, Set<Integer> partitions, URI preferredLeader) throws IOException {
        Files.createDirectories(path.resolve(USER_STATE_PATH));
        InternalState internalState = new InternalState(new ConfigState(voters), partitions, preferredLeader);
        File lockFile = path.getParent().resolve(path.getFileName() + ".lock").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
             FileChannel fileChannel = raf.getChannel()){
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

    public void flush () throws IOException {
        try {
            stateFilesLock.writeLock().lock();
            flushInternalState();
            flushUserState();
        } finally {
            stateFilesLock.writeLock().unlock();
        }
    }

    private void flushUserState(State userState) throws IOException {
        if(userState instanceof Flushable) {
            ((Flushable) userState).flush();
        }
    }

    private void flushUserState() throws IOException {
        flushUserState(userState);
    }

    private void flushInternalState() throws IOException {
        flushInternalState(internalStateFile(path), internalState);
    }

    private Path internalStateFile(Path statePath) {
        return statePath.resolve(INTERNAL_STATE_PATH).resolve(INTERNAL_STATE_FILE);
    }

    private void flushInternalState(Path internalStateFile, InternalState internalState ) throws IOException {
        metadataPersistence.save(internalStateFile, new PersistInternalState().fromInternalState(internalState));
    }

    public void recover(Path path, Properties properties)  {
        recover(path, properties, false);
    }

    public void recover(Path path, Properties properties, boolean internalStateOnly)  {
        stateFilesLock.writeLock().lock();
        try {
            recoverUnsafe(path, properties, internalStateOnly);
        } finally {
            stateFilesLock.writeLock().unlock();
        }
    }

    public void recoverUnsafe(Path path, Properties properties, boolean internalStateOnly)  {
        this.path = path;
        this.properties = properties;
        try {
            Files.createDirectories(path);
            this.internalState = recoverInternalState(internalStateFile(path));
            if(!internalStateOnly) {
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
        if(null != list) {
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

    public long lastApplied() {
        return lastIncludedIndex() + 1;
    }

    public StateResult<ER> applyEntry(JournalEntry journalEntry, Serializer<E> entrySerializer, RaftJournal journal) {
        byte [] payloadBytes = journalEntry.getPayload().getBytes();
        int partition = journalEntry.getPartition();
        int batchSize = journalEntry.getBatchSize();

        StateResult<ER> result = new StateResult<>(null);
        long stamp = stateLock.writeLock();
        try {
            if(partition< RESERVED_PARTITIONS_START) {
                E entry = entrySerializer.parse(payloadBytes);
                result = userState.execute(entry, partition, lastApplied(), batchSize, journal);
            } else if (partition == INTERNAL_PARTITION){
                applyInternalEntry(journalEntry.getPayload().getBytes());

            } else {
                for (ApplyReservedEntryInterceptor reservedEntryInterceptor : reservedEntryInterceptors) {
                    reservedEntryInterceptor.applyReservedEntry(journalEntry, lastApplied());
                }
            }
            internalState.setLastIncludedTerm(journalEntry.getTerm());
            internalState.next();
            result.setLastApplied(lastApplied());
        } finally {
            stateLock.unlockWrite(stamp);
        }
        return result;
    }


    private void applyInternalEntry(byte [] internalEntry) {
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

    private void maybeRecoverUserState() throws IOException {
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

    public StateQueryResult<QR> query(Q query, RaftJournal journal) {
        StateQueryResult<QR> result;

        long stamp = stateLock.tryOptimisticRead();
        result = new StateQueryResult<>(userState.query(query, journal), lastApplied());

        if (!stateLock.validate(stamp)) {
            stamp = stateLock.readLock();
            try {
                result = new StateQueryResult<>(userState.query(query, journal), lastApplied());
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

    public void dumpUserState(Path destPath) throws IOException {
        flush();
        try {
            stateFilesLock.readLock().lock();
            FileUtils.dump(path.resolve(USER_STATE_PATH), destPath.resolve(USER_STATE_PATH));
        } finally {
            stateFilesLock.readLock().unlock();
        }
    }


    /**
     * 列出所有复制时需要拷贝的文件。
     * @return 所有需要复制的文件的Path
     */
    private List<Path> listAllFiles(Path path) throws IOException {
        return FileUtils.listAllFiles(path);
    }

    public List<URI> voters() {
        return internalState.getConfigState().voters();
    }

    @Override
    public ReplicableIterator iterator() throws IOException {
        return new FolderTrunkIterator(path, listAllFiles(path), MAX_TRUNK_SIZE, lastIncludedIndex(), lastIncludedTerm());
    }


    public void setConfigState(ConfigState configState) {
        internalState.setConfigState(configState);
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
        if(null != userState) {
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

    public Set<Integer> getPartitions() {
        return internalState.getPartitions();
    }

    public URI getPreferredLeader() {
        return internalState.getPreferredLeader();
    }

    public JournalSnapshot getJournalSnapshot() {return internalState;
    }

    public void createSnapshot(Journal journal) throws IOException {
        internalState.setSnapshotTimestamp(System.currentTimeMillis());
        internalState.setMinOffset(
                journal.maxIndex() == internalState.minIndex() ? journal.maxOffset():
                        journal.readOffset(internalState.minIndex())
        );
        Map<Integer, Long> partitionIndices = journal.calcPartitionIndices(internalState.minOffset());
        internalState.setPartitionIndices(partitionIndices);
        flushInternalState();
    }

    public long timestamp() {
        return internalState.getSnapshotTimestamp();
    }

    @Override
    public String toString() {
        return "JournalKeeperState{" +
                "internalState=" + internalState +
                ", path=" + path +
                '}';
    }
}
