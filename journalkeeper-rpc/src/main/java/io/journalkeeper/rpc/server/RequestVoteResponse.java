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

import java.net.URI;

/**
 * @author LiYue
 * Date: 2019-03-14
 */
public class RequestVoteResponse extends BaseResponse implements Termed {

    private final int term;
    private final boolean voteGranted;
    // Transient
    private URI uri;

    public RequestVoteResponse(Throwable throwable) {
        this(throwable, -1, false);
    }


    public RequestVoteResponse(int term, boolean voteGranted) {
        this(null, term, voteGranted);
    }

    private RequestVoteResponse(Throwable throwable, int term, boolean voteGranted) {
        super(throwable);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }
}
