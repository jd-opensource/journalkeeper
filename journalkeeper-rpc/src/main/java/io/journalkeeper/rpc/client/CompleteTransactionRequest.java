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
package io.journalkeeper.rpc.client;

import java.util.UUID;

public class CompleteTransactionRequest {
    private final UUID transactionId;
    private final boolean commitOrAbort;

    public CompleteTransactionRequest(UUID transactionId, boolean commitOrAbort) {
        this.transactionId = transactionId;
        this.commitOrAbort = commitOrAbort;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public boolean isCommitOrAbort() {
        return commitOrAbort;
    }
}
