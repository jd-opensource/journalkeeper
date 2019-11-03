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
package io.journalkeeper.rpc.codec;

/**
 * @author LiYue
 * Date: 2019-03-29
 */
public class RpcTypes {
    // Client server RPCs
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
    public final static int ADD_PULL_WATCH_REQUEST = 9;
    public final static int ADD_PULL_WATCH_RESPONSE = -9;
    public final static int REMOVE_PULL_WATCH_REQUEST = 10;
    public final static int REMOVE_PULL_WATCH_RESPONSE = -10;
    public final static int PULL_EVENTS_REQUEST = 11;
    public final static int PULL_EVENTS_RESPONSE = -11;
    public static final int CONVERT_ROLL_REQUEST = 12;
    public static final int CONVERT_ROLL_RESPONSE = -12;
    public static final int GET_SERVER_STATUS_REQUEST = 13;
    public static final int GET_SERVER_STATUS_RESPONSE = -13;
    public static final int CREATE_TRANSACTION_REQUEST = 14;
    public static final int CREATE_TRANSACTION_RESPONSE = -14;
    public static final int COMPLETE_TRANSACTION_REQUEST = 15;
    public static final int COMPLETE_TRANSACTION_RESPONSE = -15;
    public static final int GET_OPENING_TRANSACTIONS_REQUEST = 16;
    public static final int GET_OPENING_TRANSACTIONS_RESPONSE = -16;



    // Server RPCs
    public final static int ASYNC_APPEND_ENTRIES_REQUEST = 101;
    public final static int ASYNC_APPEND_ENTRIES_RESPONSE = -101;
    public final static int REQUEST_VOTE_REQUEST = 102;
    public final static int REQUEST_VOTE_RESPONSE = -102;
    public final static int GET_SERVER_ENTRIES_REQUEST = 103;
    public final static int GET_SERVER_ENTRIES_RESPONSE = -103;
    public final static int GET_SERVER_STATE_REQUEST = 104;
    public final static int GET_SERVER_STATE_RESPONSE = -104;
    public final static int DISABLE_LEADER_WRITE_REQUEST = 105;
    public final static int DISABLE_LEADER_WRITE_RESPONSE = -105;

}
