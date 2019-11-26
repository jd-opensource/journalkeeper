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
package io.journalkeeper.core.strategy;

import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.utils.ThreadSafeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.SortedMap;

/**
 * @author LiYue
 * Date: 2019/11/25
 */
public class DefaultJournalCompactionStrategy implements JournalCompactionStrategy {
    private static final Logger logger = LoggerFactory.getLogger(DefaultJournalCompactionStrategy.class);
    private final int retentionMin;

    public DefaultJournalCompactionStrategy(int journalRetentionMin) {
        this.retentionMin = journalRetentionMin;
    }

    @Override
    public long calculateCompactionIndex(SortedMap<Long, Long> snapshotTimestamps, Journal journal) {
        long index = -1;
        long now = System.currentTimeMillis();

        if (retentionMin > 0) {
            long compactTimestamp = now - retentionMin * 60 * 1000L;

            for (Map.Entry<Long, Long> entry : snapshotTimestamps.entrySet()) {
                long snapshotIndex = entry.getKey();
                long snapshotTimestamp = entry.getValue();
                if (snapshotTimestamp <= compactTimestamp) {
                    index = snapshotIndex;
                } else {
                    break;
                }
            }

        }
        logger.info("Calculate journal compaction index: {}, current timestamp: {}.", index, ThreadSafeFormat.format(new Date(now)));
        return index;
    }
}
