package io.journalkeeper.rpc.client;

/**
 * Client调用Server的RPC
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClientServerRpc {

    UpdateClusterStateResponse updateClusterState(UpdateObserversRequest request);
    QueryClusterStateResponse queryClusterState(QueryClusterStateRequest request);
    QueryServerStateResponse queryServerState(QueryClusterStateRequest request);
    LastAppliedResponse lastApplied(LastAppliedRequest request);
    QuerySnapshotResponse querySnapshot(QuerySnapshotRequest request);
    GetServersResponse getServer(GetServersRequest request);
    UpdateVotersResponse updateVoters(UpdateVotersRequest request);
    UpdateObserversResponse updateObservers(UpdateObserversRequest request);
}
