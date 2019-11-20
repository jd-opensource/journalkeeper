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

import java.io.Serializable;
import java.util.Set;

import static io.journalkeeper.core.entry.reserved.ReservedEntryType.TYPE_SCALE_PARTITIONS;

/**
 * @author LiYue
 * Date: 2019-05-09
 */

public class ScalePartitionsEntry extends ReservedEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Set<Integer> partitions;
    public ScalePartitionsEntry(Set<Integer> partitions) {
        super(TYPE_SCALE_PARTITIONS);
        this.partitions = partitions;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }
}
