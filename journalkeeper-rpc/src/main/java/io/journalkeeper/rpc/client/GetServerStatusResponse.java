package io.journalkeeper.rpc.client;

import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.rpc.BaseResponse;

/**
 * @author LiYue
 * Date: 2019-09-04
 */
public class GetServerStatusResponse extends BaseResponse {
    private final ServerStatus serverStatus;

    public GetServerStatusResponse(Throwable exception) {
        this(exception, null);
    }

    public GetServerStatusResponse(ServerStatus serverStatus) {
        this(null, serverStatus);
    }
    private GetServerStatusResponse(Throwable exception, ServerStatus serverStatus) {
        super(exception);
        this.serverStatus = serverStatus;
    }

    public ServerStatus getServerStatus() {
        return serverStatus;
    }
}
