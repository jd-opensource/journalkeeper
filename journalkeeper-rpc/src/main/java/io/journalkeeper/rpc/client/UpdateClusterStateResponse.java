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

import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.StatusCode;

import java.util.Collections;
import java.util.List;

/**
 * @author LiYue
 * Date: 2019-03-14
 */
public class UpdateClusterStateResponse extends LeaderResponse {
    private final List<byte[]> results;

    public UpdateClusterStateResponse() {
        super();
        this.results = Collections.emptyList();
        ;
    }

    public UpdateClusterStateResponse(List<byte[]> results) {
        super();
        this.results = results;
    }

    public UpdateClusterStateResponse(Throwable exception) {
        super(exception);
        this.results = Collections.emptyList();

    }

    /**
     * 序列化后的执行结果。
     * @return 序列化后的执行结果。
     */
    public List<byte[]> getResults() {
        return results;
    }

    @Override
    protected void onSetException(Throwable throwable) {
        try {
            throw throwable;
        } catch (ServerBusyException e) {
            setStatusCode(StatusCode.SERVER_BUSY);
        } catch (Throwable t) {
            super.onSetException(throwable);
        }
    }

}
