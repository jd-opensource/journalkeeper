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
package com.jd.journalkeeper.rpc.server;

/**
 * @author liyue25
 * Date: 2019-03-21
 */
public class GetServerStateRequest {
    private final long lastIncludedIndex;
    private final long offset;

    public GetServerStateRequest(long lastIncludedIndex, long offset) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.offset = offset;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public long getOffset() {
        return offset;
    }
}
