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
package io.journalkeeper.rpc.server;

import io.journalkeeper.rpc.BaseResponse;

/**
 * @author LiYue
 * Date: 2019-03-14
 */
public class InstallSnapshotResponse extends BaseResponse implements Termed {
    private final int term;

    private InstallSnapshotResponse(Throwable exception, int term) {
        super(exception);
        this.term = term;
    }

    public InstallSnapshotResponse(int term) {
        this(null, term);
    }


    public InstallSnapshotResponse(Throwable exception) {
        this(exception, -1);
    }

    public int getTerm() {
        return term;
    }

}
