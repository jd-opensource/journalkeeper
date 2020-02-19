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
package io.journalkeeper.core.entry;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;

import java.nio.ByteBuffer;

/**
 * @author LiYue
 * Date: 2019/10/12
 */
public class DefaultJournalEntryParser implements JournalEntryParser {
    @Override
    public int headerLength() {
        return JournalEntryParseSupport.getHeaderLength();
    }

    @Override
    public JournalEntry parseHeader(byte[] headerBytes) {
        return new DefaultJournalEntry(headerBytes, true, false);
    }

    @Override
    public JournalEntry parse(byte[] bytes) {
        return new DefaultJournalEntry(bytes, true, true);
    }

    @Override
    public JournalEntry createJournalEntry(byte[] payload) {
        int headerLength = headerLength();

        byte[] rawEntry = new byte[headerLength + payload.length];
        ByteBuffer buffer = ByteBuffer.wrap(rawEntry);
        JournalEntryParseSupport.setInt(buffer, JournalEntryParseSupport.LENGTH, rawEntry.length);
        JournalEntryParseSupport.setShort(buffer, JournalEntryParseSupport.MAGIC, DefaultJournalEntry.MAGIC_CODE);
        JournalEntryParseSupport.setLong(buffer, JournalEntryParseSupport.TIMESTAMP, System.currentTimeMillis());

        for (int i = 0; i < payload.length; i++) {
            rawEntry[headerLength + i] = payload[i];
        }
        return parse(rawEntry);

    }
}
