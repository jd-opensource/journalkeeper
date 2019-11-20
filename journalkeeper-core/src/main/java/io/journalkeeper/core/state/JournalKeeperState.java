package io.journalkeeper.core.state;

import io.journalkeeper.base.Replicable;
import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.entry.reserved.ReservedEntriesSerializeSupport;
import io.journalkeeper.core.entry.reserved.ReservedEntryType;
import io.journalkeeper.core.entry.reserved.ScalePartitionsEntry;
import io.journalkeeper.core.entry.reserved.SetPreferredLeaderEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.StampedLock;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITIONS_START;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public class JournalKeeperState <E, ER, Q, QR> implements Replicable {
    private static final Logger logger = LoggerFactory.getLogger(JournalKeeperState.class);
    private Path path;
    private State<E, ER, Q, QR> userState;
    private InternalState internalState;
    private long lastIncludedIndex;
    private int lastIncludedTerm;
    private final Map<ReservedEntryType, ApplyInternalEntryInterceptor> internalEntryInterceptors = new HashMap<>();
    private final List<ApplyReservedEntryInterceptor> reservedEntryInterceptors = new CopyOnWriteArrayList<>();
    private final StateFactory<E, ER, Q, QR> userStateFactory;
    /**
     * TODO: 检查是否所有读写都锁住了
     * 状态读写锁
     * 读取状态是加乐观锁或读锁，变更状态时加写锁。
     */
    private final StampedLock stateLock = new StampedLock();

    public JournalKeeperState(StateFactory<E, ER, Q, QR> userStateFactory) {
        this.userStateFactory = userStateFactory;
    }


    public void init(Path path, InternalState reservedState) throws IOException {
        init(path, userStateFactory.createState(), reservedState);
    }
    public void init(Path path, State<E, ER, Q, QR> userState, InternalState reservedState) throws IOException {
        // TODO 加文件锁
        flushInternalState(path, internalState, -1L, -1);
        flushUserState(userState);
    }

    public void flush () throws IOException {
        flushInternalState();
        flushUserState();
    }

    private void flushUserState(State userState) throws IOException {
        if(userState instanceof Flushable) {
            ((Flushable) userState).flush();
        }
    }

    public void flushUserState() throws IOException {
        flushUserState(userState);
    }

    public void flushInternalState() throws IOException {
        flushInternalState(path, internalState, lastIncludedIndex, lastIncludedTerm);
    }

    private void flushInternalState(Path path, InternalState internalState, long lastIncludedIndex, int lastIncludedTerm ) {
        // TODO
    }
    public void recover(Path path, RaftJournal raftJournal, Properties properties) {
        this.path = path;
        this.internalState = recoverInternalState(path);
        this.userState = userStateFactory.createState();
        userState.recover(path, raftJournal, properties);
    }

    // TODO
    private InternalState recoverInternalState(Path path) {
        return null;
    }

    public void addInterceptor(ReservedEntryType type, ApplyInternalEntryInterceptor internalEntryInterceptor) {
        this.internalEntryInterceptors.put(type, internalEntryInterceptor);
    }

    public void removeInterceptor(ReservedEntryType type) {
        this.internalEntryInterceptors.remove(type);
    }

    public void addInterceptor(ApplyReservedEntryInterceptor interceptor) {
        reservedEntryInterceptors.add(interceptor);
    }

    public void removeInterceptor(ApplyReservedEntryInterceptor interceptor) {
        reservedEntryInterceptors.remove(interceptor);
    }
    public Path getPath() {
        return path;
    }

    public State<E, ER, Q, QR> getUserState() {
        return userState;
    }

    public InternalState getInternalState() {
        return internalState;
    }

    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int lastIncludedTerm() {
        return lastIncludedTerm;
    }

    public long lastApplied() {
        return lastIncludedIndex() + 1;
    }

    public StateResult<ER> applyEntry(JournalEntry journalEntry, Serializer<E> entrySerializer) {
        byte [] payloadBytes = journalEntry.getPayload().getBytes();
        int partition = journalEntry.getPartition();
        int batchSize = journalEntry.getBatchSize();

        StateResult<ER> result = null;
        long stamp = stateLock.writeLock();
        try {
            if(partition< RESERVED_PARTITIONS_START) {
                E entry = entrySerializer.parse(payloadBytes);
                result = userState.execute(entry, partition, lastApplied(), batchSize);
            } else if (partition == INTERNAL_PARTITION){
                applyRaftPartition(journalEntry.getPayload().getBytes());
            } else {
                for (ApplyReservedEntryInterceptor reservedEntryInterceptor : reservedEntryInterceptors) {
                    reservedEntryInterceptor.applyReservedEntry(journalEntry, lastApplied());
                }
            }
        } finally {
            stateLock.unlockWrite(stamp);
        }
        lastIncludedTerm = journalEntry.getTerm();
        lastIncludedIndex ++;
        return result;
    }


    private void applyRaftPartition(byte [] reservedEntry) {
        ReservedEntryType type = ReservedEntriesSerializeSupport.parseEntryType(reservedEntry);
        logger.info("Apply reserved entry, type: {}", type);
        switch (type) {
            case TYPE_SCALE_PARTITIONS:
                internalState.setPartitions(ReservedEntriesSerializeSupport.parse(reservedEntry, ScalePartitionsEntry.class).getPartitions());
                break;
            case TYPE_SET_PREFERRED_LEADER:
                SetPreferredLeaderEntry setPreferredLeaderEntry = ReservedEntriesSerializeSupport.parse(reservedEntry);
                URI old = internalState.getPreferredLeader();
                internalState.setPreferredLeader(setPreferredLeaderEntry.getPreferredLeader());
                logger.info("Set preferred leader from {} to {}.", old, internalState.getPreferredLeader());
                break;
            default:
        }

        ApplyInternalEntryInterceptor interceptor = internalEntryInterceptors.get(type);
        if (null != interceptor) {
            interceptor.applyInternalEntry(type, reservedEntry);
        }

        try {
            flushInternalState();
        } catch (IOException e) {
            logger.warn("Flush internal state exception! Path: {}.", path, e);
        }
    }

    public StateQueryResult<QR> queryUserState(Q query) {
        StateQueryResult<QR> result;

        long stamp = stateLock.tryOptimisticRead();
        result = new StateQueryResult<>(userState.query(query), lastApplied());

        if (!stateLock.validate(stamp)) {
            stamp = stateLock.readLock();
            try {
                result = new StateQueryResult<>(userState.query(query), lastApplied());
            } finally {
                stateLock.unlockRead(stamp);
            }
        }
        return result;
    }

    public void dump(Path tempSnapshotPath) {

        // TODO 加文件锁
    }

    public List<URI> voters() {
        return internalState.getConfigState().voters();
    }

    @Override
    public byte[] readSerializedTrunk(long offset, int size) throws IOException {
        return new byte[0];
    }

    @Override
    public long serializedDataSize() {
        return 0;
    }

    @Override
    public void installSerializedTrunk(byte[] data, long offset, boolean isLastTrunk) throws IOException {

    }

    public void setConfigState(ConfigState configState) {
        internalState.setConfigState(configState);
    }

    public void close() {
        userState.close();
    }

    public void clear() {

    }
}
