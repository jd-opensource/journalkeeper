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

import java.util.Collection;
import java.util.Collections;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public class ReservedPartition {
    public static final int RESERVED_PARTITION = 30000;

    public static void validatePartition(int partition) {
        if(partition >= RESERVED_PARTITION || partition < 0) {
            throw new IllegalArgumentException(
                    String.format("partition %d should be zero or positive number and less than %d",
                            partition, RESERVED_PARTITION)
            );
        }
    }

    public static void validatePartitions(int [] partitions) {
        for (int partition : partitions) {
            validatePartition(partition);
        }
    }

    public static void validatePartitions(Collection<Integer> partitions) {
        for (Integer partition : partitions) {
            validatePartition(partition);
        }
    }
}
