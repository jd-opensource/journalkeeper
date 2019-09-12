package io.journalkeeper.core.server;

import io.journalkeeper.core.api.RaftEntry;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.entry.EntryHeader;
import io.journalkeeper.core.entry.JournalEntryParser;
import io.journalkeeper.core.entry.reserved.ReservedEntriesSerializeSupport;
import io.journalkeeper.core.entry.reserved.ReservedEntry;
import io.journalkeeper.core.entry.reserved.UpdateVotersS1Entry;
import io.journalkeeper.core.entry.reserved.UpdateVotersS2Entry;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITION;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
class VoterConfigManager {
    private static final Logger logger = LoggerFactory.getLogger(VoterConfigManager.class);
    boolean maybeUpdateLeaderConfig(UpdateClusterStateRequest request,
                                    AbstractServer.VoterConfigurationStateMachine votersConfigStateMachine,
                                    Journal journal,
                                    Callable appendEntryCallable,
                                    URI serverUri,
                                    List<Leader.ReplicationDestination> followers,
                                    int replicationParallelism,
                                    Map<URI, JMetric> appendEntriesRpcMetricMap) throws Exception {
        // 如果是配置变更请求，立刻更新当前配置
        if(request.getPartition() == RESERVED_PARTITION ){
            int entryType = ReservedEntriesSerializeSupport.parseEntryType(request.getEntry());
            if (entryType == ReservedEntry.TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = ReservedEntriesSerializeSupport.parse(request.getEntry());
                // 等待所有日志都被提交才能进行配置变更
                waitingForAllEntriesCommitted(journal);

                votersConfigStateMachine.toJointConsensus(updateVotersS1Entry.getConfigNew(), appendEntryCallable);
                List<URI> oldFollowerUriList = followers.stream().map(Leader.ReplicationDestination::getUri).collect(Collectors.toList());
                for (URI uri : updateVotersS1Entry.getConfigNew()) {
                    if (!serverUri.equals(uri) && // uri was not me
                            !oldFollowerUriList.contains(uri)) { // and not included in the old followers collection
                        followers.add(new Leader.ReplicationDestination(uri, journal.maxIndex(), replicationParallelism));
                    }
                }
                return true;
            } else if (entryType == ReservedEntry.TYPE_UPDATE_VOTERS_S2) {
                // 等待所有日志都被提交才能进行配置变更
                waitingForAllEntriesCommitted(journal);
                votersConfigStateMachine.toNewConfig(appendEntryCallable);


                followers.removeIf(follower -> {
                    if(!votersConfigStateMachine.voters().contains(follower.getUri())){
                        appendEntriesRpcMetricMap.remove(follower.getUri());
                        return true;
                    } else {
                        return false;
                    }
                });
                return true;
            }
        }
        return false;
    }

    /**
     * Block current thread and waiting until all entries were committed。
     */
    private void waitingForAllEntriesCommitted(Journal journal) throws InterruptedException {
        long t0 = System.nanoTime();
        while(journal.commitIndex() < journal.maxIndex()) {
            if(System.nanoTime() - t0 < 10000000000L) {
                Thread.yield();
            } else {
                Thread.sleep(10L);
            }
        }
    }

    // 如果要删除部分未提交的日志，并且待删除的这部分存在配置变更日志，则需要回滚配置
    void maybeRollbackConfig(long startIndex, Journal journal,
                             AbstractServer.VoterConfigurationStateMachine votersConfigStateMachine) {
        if(startIndex >= journal.maxIndex()) {
            return;
        }
        long index = journal.maxIndex(RESERVED_PARTITION);
        long startOffset = journal.readOffset(startIndex);
        while (--index >= journal.minIndex(RESERVED_PARTITION)) {
            RaftEntry entry = journal.readByPartition(RESERVED_PARTITION, index);
            if (entry.getHeader().getOffset() < startOffset) {
                break;
            }
            int reservedEntryType = ReservedEntriesSerializeSupport.parseEntryType(entry.getEntry());
            if(reservedEntryType == ReservedEntry.TYPE_UPDATE_VOTERS_S2) {
                UpdateVotersS2Entry updateVotersS2Entry = ReservedEntriesSerializeSupport.parse(entry.getEntry());
                votersConfigStateMachine.rollbackToJointConsensus(updateVotersS2Entry.getConfigOld());
            } else if(reservedEntryType == ReservedEntry.TYPE_UPDATE_VOTERS_S1) {
                votersConfigStateMachine.rollbackToOldConfig();
            }
        }
    }
    // 非Leader（Follower和Observer）复制日志到本地后，如果日志中包含配置变更，则立即变更配置
    void maybeUpdateNonLeaderConfig(List<byte []> entries, AbstractServer.VoterConfigurationStateMachine votersConfigStateMachine) throws Exception {
        for (byte[] rawEntry : entries) {
            ByteBuffer entryBuffer = ByteBuffer.wrap(rawEntry);
            EntryHeader entryHeader = JournalEntryParser.parseHeader(entryBuffer);
            if(entryHeader.getPartition() == RESERVED_PARTITION) {
                byte [] payload = JournalEntryParser.getEntry(rawEntry);
                int entryType = ReservedEntriesSerializeSupport.parseEntryType(payload);
                if (entryType == ReservedEntry.TYPE_UPDATE_VOTERS_S1) {
                    UpdateVotersS1Entry updateVotersS1Entry = ReservedEntriesSerializeSupport.parse(payload);

                    votersConfigStateMachine.toJointConsensus(updateVotersS1Entry.getConfigNew(),
                            () -> null);
                } else if (entryType == ReservedEntry.TYPE_UPDATE_VOTERS_S2) {
                    votersConfigStateMachine.toNewConfig(() -> null);
//                    // Stop myself if I'm no longer a member of the cluster.
//                    if(roll == RaftServer.Roll.VOTER && !votersConfigStateMachine.voters().contains(serverUri)) {
//                        server.stopAsync();
//                        break;
//                    }
                }
            }
        }
    }

    void applyReservedEntry(int type, byte [] reservedEntry, VoterState voterState,
                                      AbstractServer.VoterConfigurationStateMachine votersConfigStateMachine,
                                      ClientServerRpc clientServerRpc, URI serverUri, StateServer server) {
        switch (type) {
            case ReservedEntry.TYPE_UPDATE_VOTERS_S1:
                if(voterState == VoterState.LEADER) {
                    byte[] s2Entry = ReservedEntriesSerializeSupport.serialize(new UpdateVotersS2Entry(votersConfigStateMachine.getConfigOld(), votersConfigStateMachine.getConfigNew()));
                    try {
                        if (votersConfigStateMachine.isJointConsensus()) {
                            clientServerRpc.updateClusterState(new UpdateClusterStateRequest(s2Entry, RESERVED_PARTITION, 1, ResponseConfig.ONE_WAY));
                        } else {
                            throw new IllegalStateException();
                        }
                    } catch (Exception e) {
                        UpdateVotersS1Entry updateVotersS1Entry = ReservedEntriesSerializeSupport.parse(reservedEntry);
                        logger.warn("Failed to update voter config in step 1! Config in the first step entry from: {} To: {}, " +
                                        "voter config old: {}, new: {}.",
                                updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew(),
                                votersConfigStateMachine.getConfigOld(), votersConfigStateMachine.getConfigNew(), e);
                    }
                } else if(!votersConfigStateMachine.voters().contains(serverUri)) {
                    // Stop myself if I'm no longer a member of the cluster. Any follower can be stopped safely on this stage.
                    server.stopAsync();
                }
                break;
            case ReservedEntry.TYPE_UPDATE_VOTERS_S2:
                // Stop myself if I'm no longer a member of the cluster.
                // Leader can be stopped on this stage, a new leader will be elected in the new configuration.
                if(!votersConfigStateMachine.voters().contains(serverUri)) {
                    server.stopAsync();
                }
                break;
            default:
        }
    }

}
