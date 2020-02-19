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
