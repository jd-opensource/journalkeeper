package io.journalkeeper.core.state;

import io.journalkeeper.base.Replicable;
import io.journalkeeper.base.ReplicableIterator;
import io.journalkeeper.core.api.EntryFuture;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.exceptions.StateQueryException;
import io.journalkeeper.exceptions.StateRecoverException;
import io.journalkeeper.persistence.MetadataPersistence;
import io.journalkeeper.utils.files.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2020/4/17
 */
public class Snapshot extends JournalKeeperState implements Replicable {
private static final Logger logger = LoggerFactory.getLogger(Snapshot.class);
    private static final String SNAPSHOT_FILE = "snapshot";
    private AtomicBoolean isUserStateAvailable = new AtomicBoolean(false);
    private static final int MAX_TRUNK_SIZE = 1024 * 1024;

    public Snapshot(StateFactory userStateFactory, MetadataPersistence metadataPersistence) {
        super(userStateFactory, metadataPersistence);
    }

    @Override
    public void recover(Path path, Properties properties) {
        File snapshotFile = path.resolve(SNAPSHOT_FILE).toFile();
        if (!snapshotFile.isFile()) {
            throw new StateRecoverException(
                    String.format("Incomplete snapshot: %s!", path.toString())
            );
        }
        super.recover(path, properties, false);
    }

    public void createSnapshot(Journal journal) throws IOException {
        internalState.setSnapshotTimestamp(System.currentTimeMillis());
        internalState.setMinOffset(
                journal.maxIndex() == internalState.minIndex() ? journal.maxOffset() :
                        journal.readOffset(internalState.minIndex())
        );
        Map<Integer, Long> partitionIndices = journal.calcPartitionIndices(internalState.minOffset());
        internalState.setPartitionIndices(partitionIndices);
        flushInternalState();
    }

    public void createFirstSnapshot(Set<Integer> partitions) throws IOException{
        internalState.setSnapshotTimestamp(System.currentTimeMillis());
        internalState.setMinOffset(0L);
        Map<Integer, Long> partitionIndices = partitions.stream().collect(Collectors.toMap(p -> p, p -> 0L));
        internalState.setPartitionIndices(partitionIndices);
        flushInternalState();
    }
    public long timestamp() {
        return internalState.getSnapshotTimestamp();
    }

    private void maybeRecoverUserState() throws IOException {

        if(isUserStateAvailable.compareAndSet(false, true)) {
            recoverUserState();
        }
        isUserStateAvailable.set(true);
    }

    @Override
    public StateResult applyEntry(JournalEntry entryHeader, EntryFuture entryFuture, RaftJournal journal) {
        throw new UnsupportedOperationException("Apply entry on a snapshot is now allowed!");
    }

    @Override
    public StateQueryResult query(byte[] query, RaftJournal journal) {
        try {
            maybeRecoverUserState();
            return super.query(query, journal);
        } catch (IOException e) {
            throw new StateQueryException(e);
        }
    }

    public void dumpUserState(Path destPath) throws IOException {

        FileUtils.dump(path.resolve(USER_STATE_PATH), destPath.resolve(USER_STATE_PATH));
    }

    public static void markComplete(Path path) throws IOException {
        FileUtils.createIfNotExists(path.resolve(SNAPSHOT_FILE));
    }


    @Override
    public ReplicableIterator iterator() throws IOException {
        return new FolderTrunkIterator(path, listAllFiles(path), MAX_TRUNK_SIZE, lastIncludedIndex(), lastIncludedTerm());
    }

    /**
     * 列出所有复制时需要拷贝的文件。
     * @return 所有需要复制的文件的Path
     */
    private List<Path> listAllFiles(Path path) throws IOException {
        List<Path> allFiles = new ArrayList<>(FileUtils.listAllFiles(path));
        if (allFiles.remove(path.resolve(SNAPSHOT_FILE))) {
            allFiles.add(path.resolve(SNAPSHOT_FILE));
        }
        return allFiles;
    }
}
