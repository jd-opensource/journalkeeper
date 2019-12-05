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

import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.utils.format.Format;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2019-08-14
 */
class Callback {
    private final long position;
    private final long timestamp;
    private final ResponseFuture responseFuture;

    Callback(long position, ResponseFuture responseFuture) {
        this.position = position;
        this.timestamp = System.currentTimeMillis();
        this.responseFuture = responseFuture;
    }

    long getPosition() {
        return position;
    }


    long getTimestamp() {
        return timestamp;
    }

    public ResponseFuture getResponseFuture() {
        return responseFuture;
    }

    @Override
    public String toString() {
        return "Callback{" +
                "position=" + position +
                ", timestamp=" + Format.format(new Date(timestamp)) +
                '}';
    }
}
