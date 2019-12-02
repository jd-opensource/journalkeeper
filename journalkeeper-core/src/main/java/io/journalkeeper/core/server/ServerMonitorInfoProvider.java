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

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.JournalKeeperState;
import io.journalkeeper.monitor.FollowerMonitorInfo;
import io.journalkeeper.monitor.JournalMonitorInfo;
import io.journalkeeper.monitor.JournalPartitionMonitorInfo;
import io.journalkeeper.monitor.LeaderFollowerMonitorInfo;
import io.journalkeeper.monitor.LeaderMonitorInfo;
import io.journalkeeper.monitor.MonitoredServer;
import io.journalkeeper.monitor.NodeMonitorInfo;
import io.journalkeeper.monitor.ServerMonitorInfo;
import io.journalkeeper.monitor.VoterMonitorInfo;
import io.journalkeeper.persistence.JournalPersistence;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.journalkeeper.core.journal.Journal.INDEX_STORAGE_SIZE;

/**
 * @author LiYue
 * Date: 2019/11/19
 */
public class ServerMonitorInfoProvider implements MonitoredServer {
    private final Server server;

    public ServerMonitorInfoProvider(Server server) {
        this.server = server;
    }

    @Override
    public URI uri() {
        return server.serverUri();
    }

    @Override
    public ServerMonitorInfo collect() {
        ServerMonitorInfo serverMonitorInfo = new ServerMonitorInfo();
        serverMonitorInfo.setUri(server.serverUri());
        serverMonitorInfo.setState(server.serverState());
        serverMonitorInfo.setRoll(server.roll());

        AbstractServer abstractServer = server.getServer();
        if (null != abstractServer) {
            serverMonitorInfo.setLeader(abstractServer.getLeaderUri());
            NodeMonitorInfo nodeMonitorInfo = collectNodeMonitorInfo(abstractServer.getState().getConfigState());
            serverMonitorInfo.setNodes(nodeMonitorInfo);
            JournalMonitorInfo journalMonitorInfo = collectJournalMonitorInfo(abstractServer.getJournal(), abstractServer.getState());
            serverMonitorInfo.setJournal(journalMonitorInfo);

            if (server.roll() == RaftServer.Roll.VOTER) {
                Voter voter = (Voter) abstractServer;
                VoterMonitorInfo voterMonitorInfo = collectVoterMonitorInfo(voter);
                serverMonitorInfo.setVoter(voterMonitorInfo);
            }
        }

        return serverMonitorInfo;
    }

    private VoterMonitorInfo collectVoterMonitorInfo(Voter voter) {
        VoterMonitorInfo voterMonitorInfo = new VoterMonitorInfo();
        voterMonitorInfo.setState(voter.getVoterState());
        voterMonitorInfo.setLastVote(voter.getLastVote());
        voterMonitorInfo.setElectionTimeout(voter.getElectionTimeoutMs());
        voterMonitorInfo.setNextElectionTime(voter.getNextElectionTime());
        voterMonitorInfo.setLastHeartbeat(voter.getLastHeartbeat());
        voterMonitorInfo.setPreferredLeader(voter.getPreferredLeader());
        if(voter.getVoterState() == VoterState.LEADER) {
            Leader leader = voter.getLeader();
            LeaderMonitorInfo leaderMonitorInfo;
            leaderMonitorInfo = collectLeaderMonitorInfo(leader);
            voterMonitorInfo.setLeader(leaderMonitorInfo);
        } else if (voter.getVoterState() == VoterState.FOLLOWER) {
            Follower follower = voter.getFollower();
            FollowerMonitorInfo followerMonitorInfo;
            followerMonitorInfo = collectFollowerMonitorInfo(follower);
            voterMonitorInfo.setFollower(followerMonitorInfo);
        }
        return voterMonitorInfo;
    }

    private FollowerMonitorInfo collectFollowerMonitorInfo(Follower follower) {
        FollowerMonitorInfo followerMonitorInfo = null;
        if (null != follower) {
            followerMonitorInfo = new FollowerMonitorInfo();
            followerMonitorInfo.setState(follower.serverState());
            followerMonitorInfo.setReplicationQueueSize(follower.getReplicationQueueSize());
            followerMonitorInfo.setLeaderMaxIndex(follower.getLeaderMaxIndex());
        }
        return followerMonitorInfo;
    }

    private LeaderMonitorInfo collectLeaderMonitorInfo(Leader leader) {
        LeaderMonitorInfo leaderMonitorInfo = null;
        if(null != leader) {
            leaderMonitorInfo = new LeaderMonitorInfo();
            leaderMonitorInfo.setState(leader.serverState());
            leaderMonitorInfo.setRequestQueueSize(leader.getRequestQueueSize());
            leaderMonitorInfo.setWriteEnabled(leader.isWriteEnabled());
            @SuppressWarnings("unchecked")
            List<Leader.ReplicationDestination> replicationDestinations = leader.getFollowers();
            if(null != replicationDestinations) {
                List<LeaderFollowerMonitorInfo> leaderFollowerMonitorInfoList = new ArrayList<>(replicationDestinations.size());
                for (Leader.ReplicationDestination destination : replicationDestinations) {
                    LeaderFollowerMonitorInfo destInfo = collectLeaderFollowerMonitorInfo(destination);
                    leaderFollowerMonitorInfoList.add(destInfo);
                }
                leaderMonitorInfo.setFollowers(leaderFollowerMonitorInfoList);
            }
        }
        return leaderMonitorInfo;
    }

    private LeaderFollowerMonitorInfo collectLeaderFollowerMonitorInfo(Leader.ReplicationDestination destination) {
        LeaderFollowerMonitorInfo destInfo = new LeaderFollowerMonitorInfo();
        destInfo.setUri(destination.getUri());
        destInfo.setNextIndex(destination.getNextIndex());
        destInfo.setMatchIndex(destination.getMatchIndex());
        destInfo.setRepStartIndex(destination.getRepStartIndex());
        destInfo.setLastHeartbeatResponseTime(destination.getLastHeartbeatResponseTime());
        destInfo.setLastHeartbeatRequestTime(destination.getLastHeartbeatRequestTime());
        return destInfo;
    }

    private NodeMonitorInfo collectNodeMonitorInfo(ConfigState voterConfigurationStateMachine) {
        NodeMonitorInfo nodeMonitorInfo = null;
        if (null != voterConfigurationStateMachine) {
            nodeMonitorInfo = new NodeMonitorInfo();
            nodeMonitorInfo.setJointConsensus(voterConfigurationStateMachine.isJointConsensus());
            if(voterConfigurationStateMachine.isJointConsensus()) {
                nodeMonitorInfo.setNewConfig(voterConfigurationStateMachine.getConfigNew());
                nodeMonitorInfo.setOldConfig(voterConfigurationStateMachine.getConfigOld());
            } else {
                nodeMonitorInfo.setConfig(voterConfigurationStateMachine.getConfigNew());
            }
        }
        return nodeMonitorInfo;
    }

    private JournalMonitorInfo collectJournalMonitorInfo(Journal journal, JournalKeeperState state) {
        JournalMonitorInfo journalMonitorInfo = new JournalMonitorInfo();
        if(null != journal) {
            journalMonitorInfo.setMinIndex(journal.minIndex());
            journalMonitorInfo.setMaxIndex(journal.maxIndex());
            journalMonitorInfo.setFlushIndex(journal.flushedIndex());
            journalMonitorInfo.setCommitIndex(journal.commitIndex());
            JournalPersistence journalPersistence = journal.getJournalPersistence();
            journalMonitorInfo.setMinOffset(journalPersistence.min());
            journalMonitorInfo.setMaxOffset(journalPersistence.max());
            journalMonitorInfo.setFlushOffset(journalPersistence.flushed());
            JournalPersistence indexPersistence = journal.getIndexPersistence();
            journalMonitorInfo.setIndexMinOffset(indexPersistence.min());
            journalMonitorInfo.setIndexMaxOffset(indexPersistence.max());
            journalMonitorInfo.setIndexFlushOffset(indexPersistence.flushed());

            Map<Integer, JournalPersistence> partitionMap = journal.getPartitionMap();
            if (null != partitionMap) {
                List<JournalPartitionMonitorInfo> partitionMonitorInfoList = new ArrayList<>(partitionMap.size());
                partitionMap.forEach((partition, persistence) -> {
                    JournalPartitionMonitorInfo partitionMonitorInfo = new JournalPartitionMonitorInfo();
                    partitionMonitorInfo.setPartition(partition);
                    partitionMonitorInfo.setMinIndex(persistence.min() / INDEX_STORAGE_SIZE);
                    partitionMonitorInfo.setMaxIndex(persistence.max() / INDEX_STORAGE_SIZE);
                    partitionMonitorInfo.setMinOffset(persistence.min());
                    partitionMonitorInfo.setMaxOffset(persistence.max());
                    partitionMonitorInfo.setFlushOffset(persistence.flushed());
                    partitionMonitorInfoList.add(partitionMonitorInfo);
                });
                journalMonitorInfo.setPartitions(partitionMonitorInfoList);
            }
        }

        if(null != state) {
            journalMonitorInfo.setAppliedIndex(state.lastApplied());
        }
        return journalMonitorInfo;
    }
}
