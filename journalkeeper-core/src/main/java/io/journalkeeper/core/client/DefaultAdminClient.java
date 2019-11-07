package io.journalkeeper.core.client;

import io.journalkeeper.base.VoidSerializer;
import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.entry.reserved.CompactJournalEntry;
import io.journalkeeper.core.entry.reserved.ReservedEntriesSerializeSupport;
import io.journalkeeper.core.entry.reserved.ReservedPartition;
import io.journalkeeper.core.entry.reserved.ScalePartitionsEntry;
import io.journalkeeper.core.entry.reserved.SetPreferredLeaderEntry;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ConvertRollRequest;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.UpdateVotersRequest;
import io.journalkeeper.utils.threads.NamedThreadFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public class DefaultAdminClient extends AbstractClient implements AdminClient {

    private final Config config;
    private final Executor executor;


    public DefaultAdminClient(ClientRpc clientRpc, Properties properties) {
        super(clientRpc);
        this.config = toConfig(properties);
        this.executor = Executors.newFixedThreadPool(config.getThreads(), new NamedThreadFactory("Admin-Client-Executors"));
    }

    @Override
    public CompletableFuture<ClusterConfiguration> getClusterConfiguration() {
        return clientRpc.invokeClientLeaderRpc(ClientServerRpc::getServers)
                .thenApply(GetServersResponse::getClusterConfiguration);
    }

    @Override
    public CompletableFuture<ClusterConfiguration> getClusterConfiguration(URI uri) {
        return clientRpc.invokeClientServerRpc(uri, ClientServerRpc::getServers)
                .thenApply(GetServersResponse::getClusterConfiguration);
    }

    @Override
    public CompletableFuture<Boolean> updateVoters(List<URI> oldConfig, List<URI> newConfig) {
        return  clientRpc.invokeClientLeaderRpc(leaderRpc -> leaderRpc.updateVoters(new UpdateVotersRequest(oldConfig, newConfig)))
                .thenApply(BaseResponse::success);
    }

    @Override
    public CompletableFuture<Void> convertRoll(URI uri, RaftServer.Roll roll) {
        return clientRpc.invokeClientServerRpc(uri, rpc -> rpc.convertRoll(new ConvertRollRequest(roll)))
                .thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> compact(Map<Integer, Long> toIndices) {
        return this.update(ReservedEntriesSerializeSupport.serialize(new CompactJournalEntry(toIndices)),
                RaftJournal.RAFT_PARTITION, 1, ResponseConfig.REPLICATION, VoidSerializer.getInstance());
    }
    @Override
    public CompletableFuture<Void> scalePartitions(int[] partitions) {

        ReservedPartition.validatePartitions(partitions);

        return this.update(ReservedEntriesSerializeSupport.serialize(new ScalePartitionsEntry(partitions)),
                RaftJournal.RAFT_PARTITION, 1, ResponseConfig.REPLICATION, VoidSerializer.getInstance());
    }

    @Override
    public CompletableFuture<ServerStatus> getServerStatus(URI uri) {
        return clientRpc.invokeClientServerRpc(uri,
                ClientServerRpc::getServerStatus)
                .thenApply(GetServerStatusResponse::getServerStatus);
    }

    @Override
    public CompletableFuture<Void> setPreferredLeader(URI preferredLeader) {
        return this.update(ReservedEntriesSerializeSupport.serialize(new SetPreferredLeaderEntry(preferredLeader)),
                RaftJournal.RAFT_PARTITION, 1, ResponseConfig.REPLICATION, VoidSerializer.getInstance());
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setThreads(Integer.parseInt(
                properties.getProperty(
                        DefaultRaftClient.Config.THREADS_KEY,
                        String.valueOf(DefaultRaftClient.Config.DEFAULT_THREADS))));

        return config;
    }

    static class Config {
        final static int DEFAULT_THREADS = 8;

        final static String THREADS_KEY = "client_admin_threads";

        private int threads = DEFAULT_THREADS;

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }
    }

}
