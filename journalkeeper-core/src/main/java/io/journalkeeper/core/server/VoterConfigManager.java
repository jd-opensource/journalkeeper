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
package io.journalkeeper.core.server;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.UpdateRequest;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.entry.internal.*;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.entry.internal.InternalEntryType.*;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
public class VoterConfigManager {
    private static final Logger logger = LoggerFactory.getLogger(VoterConfigManager.class);

    private final JournalEntryParser journalEntryParser;

    VoterConfigManager(JournalEntryParser journalEntryParser) {
        this.journalEntryParser = journalEntryParser;
    }

    boolean maybeUpdateLeaderConfig(UpdateRequest request,
                                    ConfigState votersConfigStateMachine,
                                    Journal journal,
                                    Callable appendEntryCallable,
                                    URI serverUri,
                                    List<Leader.ReplicationDestination> followers,
                                    int replicationParallelism,
                                    Map<URI, JMetric> appendEntriesRpcMetricMap) throws Exception {
        // 如果是配置变更请求，立刻更新当前配置
        if(request.getPartition() == INTERNAL_PARTITION){
            InternalEntryType entryType = InternalEntriesSerializeSupport.parseEntryType(request.getEntry());
            if (entryType == TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(request.getEntry());
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
            } else if (entryType == TYPE_UPDATE_VOTERS_S2) {
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
                             ConfigState votersConfigStateMachine) {
        if(startIndex >= journal.maxIndex()) {
            return;
        }
        long index = journal.maxIndex(INTERNAL_PARTITION);
        long startOffset = journal.readOffset(startIndex);
        while (--index >= journal.minIndex(INTERNAL_PARTITION)) {
            JournalEntry entry = journal.readByPartition(INTERNAL_PARTITION, index);
            if (entry.getOffset() < startOffset) {
                break;
            }
            InternalEntryType reservedEntryType = InternalEntriesSerializeSupport.parseEntryType(entry.getPayload().getBytes());
            if(reservedEntryType == TYPE_UPDATE_VOTERS_S2) {
                UpdateVotersS2Entry updateVotersS2Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                votersConfigStateMachine.rollbackToJointConsensus(updateVotersS2Entry.getConfigOld());
            } else if(reservedEntryType == TYPE_UPDATE_VOTERS_S1) {
                votersConfigStateMachine.rollbackToOldConfig();
            }
        }
    }
    // 非Leader（Follower和Observer）复制日志到本地后，如果日志中包含配置变更，则立即变更配置
    void maybeUpdateNonLeaderConfig(List<byte []> entries, ConfigState votersConfigStateMachine) throws Exception {
        for (byte[] rawEntry : entries) {
            JournalEntry entryHeader = journalEntryParser.parseHeader(rawEntry);
            if(entryHeader.getPartition() == INTERNAL_PARTITION) {
                int headerLength = journalEntryParser.headerLength();
                InternalEntryType entryType = InternalEntriesSerializeSupport.parseEntryType(rawEntry, headerLength, rawEntry.length - headerLength);
                if (entryType == TYPE_UPDATE_VOTERS_S1) {
                    UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(rawEntry, headerLength, rawEntry.length - headerLength);

                    votersConfigStateMachine.toJointConsensus(updateVotersS1Entry.getConfigNew(),
                            () -> null);
                } else if (entryType == TYPE_UPDATE_VOTERS_S2) {
                    votersConfigStateMachine.toNewConfig(() -> null);
                }
            }
        }
    }

    void applyReservedEntry(InternalEntryType type, byte [] reservedEntry, VoterState voterState,
                            ConfigState votersConfigStateMachine,
                            ClientServerRpc clientServerRpc, URI serverUri, StateServer server) {
        switch (type) {
            case TYPE_UPDATE_VOTERS_S1:
                if(voterState == VoterState.LEADER) {
                    byte[] s2Entry = InternalEntriesSerializeSupport.serialize(new UpdateVotersS2Entry(votersConfigStateMachine.getConfigOld(), votersConfigStateMachine.getConfigNew()));
                    try {
                        if (votersConfigStateMachine.isJointConsensus()) {
                            clientServerRpc.updateClusterState(new UpdateClusterStateRequest(
                                    Collections.singletonList(
                                            new UpdateRequest(
                                                    s2Entry, INTERNAL_PARTITION, 1
                                            )
                                    )
                                    , false, ResponseConfig.ONE_WAY));
                        } else {
                            throw new IllegalStateException();
                        }
                    } catch (Exception e) {
                        UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(reservedEntry);
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
            case TYPE_UPDATE_VOTERS_S2:
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
