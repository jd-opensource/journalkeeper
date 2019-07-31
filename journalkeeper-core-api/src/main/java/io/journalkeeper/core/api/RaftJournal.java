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

import java.util.List;
import java.util.Set;

/**
 * @author LiYue
 * Date: 2019-04-24
 */
public interface RaftJournal {
    int DEFAULT_PARTITION = 0;
    int RESERVED_PARTITION = Short.MAX_VALUE;
    long minIndex();

    long maxIndex();

    long minIndex(int partition);

    long maxIndex(int partition);

    RaftEntry readByPartition(int partition, long index);

    List<RaftEntry> readByPartition(int partition, long index, int maxSize);

    RaftEntry read(long index);

    List<RaftEntry> batchRead(long index, int size);

    RaftEntryHeader readEntryHeader(long index);

    Set<Integer> getPartitions();
}
