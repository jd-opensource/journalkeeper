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
package io.journalkeeper.core.entry;


import io.journalkeeper.core.api.RaftEntry;

import java.nio.ByteBuffer;

/**
 * 每个Entry 包括：
 *
 * Length: 4 bytes
 * Magic: 2 bytes
 * Term: 4 bytes
 * Partition: 2 bytes
 * Batch size: 2 bytes
 * Entry: Variable length
 *
 * @author LiYue
 * Date: 2019-03-19
 */
public class Entry extends RaftEntry {

    public Entry(){
        setHeader(new EntryHeader());
    }

    public Entry(EntryHeader header, byte [] entry) {
        setHeader(header);
        setEntry(entry);
    }
    public Entry(byte [] entry, int term, int partition) {
        this(entry, term, partition, 1);
    }

    public Entry(byte [] entry, int term, int partition, int batchSize){
        this();
        setEntry(entry);
        ((EntryHeader) getHeader()).setTerm(term);
        getHeader().setPayloadLength(entry.length);
        getHeader().setPartition(partition);
        getHeader().setBatchSize(batchSize);
    }


}
