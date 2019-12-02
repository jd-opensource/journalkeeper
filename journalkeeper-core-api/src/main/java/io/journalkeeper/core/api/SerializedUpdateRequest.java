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
package io.journalkeeper.core.api;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.UpdateRequest;

/**
 * @author LiYue
 * Date: 2019/11/14
 */
public class SerializedUpdateRequest extends UpdateRequest<byte[]> {
    public <E> SerializedUpdateRequest(UpdateRequest<E> updateRequest, Serializer<E> serializer) {
        super(serializer.serialize(updateRequest.getEntry()), updateRequest.getPartition(), updateRequest.getBatchSize());
    }

    public SerializedUpdateRequest(byte [] entry , int partition, int batchSize) {
        super(entry, partition, batchSize);
    }
}
