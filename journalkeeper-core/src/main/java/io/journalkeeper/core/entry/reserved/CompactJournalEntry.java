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
package io.journalkeeper.core.entry.reserved;

import java.util.Map;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class CompactJournalEntry extends ReservedEntry {
    private final Map<Integer, Long> compactIndices;
    public CompactJournalEntry(Map<Integer, Long> compactIndices) {
        super(TYPE_COMPACT_JOURNAL);
        this.compactIndices = compactIndices;
    }

    public Map<Integer, Long> getCompactIndices() {
        return compactIndices;
    }
}
