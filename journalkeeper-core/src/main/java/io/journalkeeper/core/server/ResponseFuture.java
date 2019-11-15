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
package io.journalkeeper.core.server;

import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2019/11/14
 */
class ResponseFuture {
    private final CompletableFuture<UpdateClusterStateResponse> responseFuture;
    private final CompletableFuture<UpdateClusterStateResponse> flushFuture;
    private final CompletableFuture<UpdateClusterStateResponse> replicationFuture;
    private int flushCountDown, replicationCountDown;
    private List<byte []> results;
    ResponseFuture(ResponseConfig responseConfig, int count) {
        this.flushFuture = new CompletableFuture<>();
        this.replicationFuture = new CompletableFuture<>();

        switch (responseConfig) {
            case PERSISTENCE:
                responseFuture = flushFuture;
                break;
            case ALL:
                responseFuture = new CompletableFuture<>();
                this.replicationFuture.whenComplete((replicationResponse, e) -> {
                    if (null == e) {
                        if (replicationResponse.success()) {
                            this.flushFuture.whenComplete((flushResponse, t) -> {
                                if (null == t) {
                                    if (flushResponse.success()) {
                                        // 如果都成功，优先使用replication response，因为里面有执行状态机的返回值。
                                        responseFuture.complete(replicationResponse);
                                    } else {
                                        // replication 成功，flush 失败，返回失败的flush response。
                                        responseFuture.complete(flushResponse);
                                    }
                                } else {
                                    responseFuture.complete(new UpdateClusterStateResponse(t));
                                }
                            });
                        } else {
                            responseFuture.complete(replicationResponse);
                        }
                    } else {
                        responseFuture.complete(new UpdateClusterStateResponse(e));
                    }
                });
                break;
            default:
                responseFuture = replicationFuture;
                break;

        }

        flushCountDown = count;
        replicationCountDown = count;
        results = new ArrayList<>(count);
    }

    CompletableFuture<UpdateClusterStateResponse> getResponseFuture() {
        return responseFuture;
    }

    void countDownFlush() {
        if(--flushCountDown == 0) {
            flushFuture.complete(new UpdateClusterStateResponse(Collections.emptyList()));
        }
    }

    void putResult(byte [] result) {
        results.add(result);
        if(--replicationCountDown == 0) {
            replicationFuture.complete(new UpdateClusterStateResponse(results));
        }
    }

    void completedExceptionally(Throwable throwable) {
        responseFuture.completeExceptionally(throwable);
    }
}
