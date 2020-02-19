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
package io.journalkeeper.core.api;

/**
 * @author LiYue
 * Date: 2019/10/12
 */
public interface JournalEntryParser {
    int headerLength();

    JournalEntry parse(byte[] bytes);

    default JournalEntry parseHeader(byte[] headerBytes) {
        return parse(headerBytes);
    }

    default JournalEntry createJournalEntry(byte[] payload) {
        int headerLength = headerLength();
        byte[] rawEntry = new byte[headerLength + payload.length];
        for (int i = 0; i < payload.length; i++) {
            rawEntry[headerLength + i] = payload[i];
        }
        return parse(rawEntry);
    }
}
