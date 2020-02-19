/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.client;

/**
 * RPC 方法
 * {@link ClientServerRpc#queryServerState(QueryStateRequest) queryServerState}
 * {@link ClientServerRpc#queryClusterState(QueryStateRequest) queryClusterState}
 * {@link ClientServerRpc#querySnapshot(QueryStateRequest) querySnapshot}
 * 请求参数。
 *
 * @author LiYue
 * Date: 2019-03-14
 */
public class QueryStateRequest {
    private final byte[] query;
    private final long index;

    public QueryStateRequest(byte[] query, long index) {
        this.query = query;
        this.index = index;
    }

    public QueryStateRequest(byte[] query) {
        this(query, -1L);
    }

    /**
     * 序列化后的查询条件。
     * @return 序列化后的查询条件。
     */
    public byte[] getQuery() {
        return query;
    }

    /**
     * Snapshot对应Journal的索引序号。
     * @return 索引序号。
     */
    public long getIndex() {
        return index;
    }
}
