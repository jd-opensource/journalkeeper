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
package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.rpc.LeaderResponse;
import com.jd.journalkeeper.rpc.StatusCode;

/**
 * RPC 方法
 * {@link ClientServerRpc#queryServerState(QueryStateRequest) queryServerState}
 * {@link ClientServerRpc#queryClusterState(QueryStateRequest) queryClusterState}
 * {@link ClientServerRpc#querySnapshot(QueryStateRequest) querySnapshot}
 * 返回结果。
 * * @author liyue25
 * Date: 2019-03-14
 */
public class QueryStateResponse  extends LeaderResponse {
    private final byte [] result;
    private final long lastApplied;
    public QueryStateResponse(Throwable t) {
        this(null, -1,  t);
    }

    public QueryStateResponse(byte [] result, long lastApplied){
        this(result, lastApplied, null);
    }
    public QueryStateResponse(byte [] result){
        this(result, -1L, null);
    }

    private QueryStateResponse(byte [] result, long lastApplied, Throwable t) {
        super(t);
        this.result = result;
        this.lastApplied = lastApplied;
    }

    /**
     * 序列化后的查询结果。
     * @return 序列化后的查询结果。
     */
    public byte [] getResult() {
        return result;
    }

    /**
     *
     * @return state对应的Journal索引序号
     */
    public long getLastApplied() {
        return lastApplied;
    }

    @Override
    public void setException(Throwable throwable) {
        try {
            throw throwable;
        } catch (IndexOverflowException e) {
            setStatusCode(StatusCode.INDEX_OVERFLOW);
        } catch (IndexUnderflowException e) {
            setStatusCode(StatusCode.INDEX_UNDERFLOW);
        } catch (Throwable t) {
            super.setException(throwable);
        }
    }
}
