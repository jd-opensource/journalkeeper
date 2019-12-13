package io.journalkeeper.rpc.client;

import io.journalkeeper.core.api.SnapshotsEntry;
import io.journalkeeper.rpc.LeaderResponse;

/**
 * GetSnapshotsResponse
 * author: gaohaoxiang
 * date: 2019/12/13
 */
public class GetSnapshotsResponse extends LeaderResponse {

    private SnapshotsEntry snapshots;

    public GetSnapshotsResponse(Throwable exception) {
        super(exception);
    }

    public GetSnapshotsResponse(SnapshotsEntry snapshots) {
        this.snapshots = snapshots;
    }

    public SnapshotsEntry getSnapshots() {
        return snapshots;
    }
}