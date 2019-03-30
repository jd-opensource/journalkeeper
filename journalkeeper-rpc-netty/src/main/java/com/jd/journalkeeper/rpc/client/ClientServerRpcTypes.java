package com.jd.journalkeeper.rpc.client;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class ClientServerRpcTypes {
    public final static int UPDATE_CLUSTER_STATE_REQUEST = 1;
    public final static int UPDATE_CLUSTER_STATE_RESPONSE = -1;
    public final static int QUERY_CLUSTER_STATE_REQUEST = 2;
    public final static int QUERY_CLUSTER_STATE_RESPONSE = -2;
    public final static int QUERY_SERVER_STATE_REQUEST = 3;
    public final static int QUERY_SERVER_STATE_RESPONSE = -3;
    public final static int LAST_APPLIED_REQUEST = 4;
    public final static int LAST_APPLIED_RESPONSE = -4;
    public final static int QUERY_SNAPSHOT_REQUEST = 5;
    public final static int QUERY_SNAPSHOT_RESPONSE = -5;
    public final static int GET_SERVERS_REQUEST = 6;
    public final static int GET_SERVERS_RESPONSE = -6;
    public final static int UPDATE_VOTERS_REQUEST = 7;
    public final static int UPDATE_VOTERS_RESPONSE = -7;
    public final static int UPDATE_OBSERVERS_REQUEST = 8;
    public final static int UPDATE_OBSERVERS_RESPONSE = -8;
}
