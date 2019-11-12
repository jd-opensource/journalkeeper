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
package io.journalkeeper.utils.retry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-09-17
 */
public class RandomDestinationSelector<D> implements DestinationSelector<D> {
    private  Collection<D> allDestinations;

    public RandomDestinationSelector(Collection<D> allDestinations) {
        this.allDestinations = allDestinations;
    }

    @Override
    public D select(Set<D> usedDestinations) {
        // 尽量不选择已经用过的
        List<D> forSelected =  allDestinations.stream()
                .filter(d -> !usedDestinations.contains(d))
                .collect(Collectors.toList());
        // 如果都用过了，那就都可以选
        if(forSelected.size() == 0) {
            forSelected = new ArrayList<>(allDestinations);
        }
        if(forSelected.size() > 0) {
            return forSelected.get(ThreadLocalRandom.current().nextInt(forSelected.size()));
        } else {
            return null;
        }

    }

    public Collection<D> getAllDestinations() {
        return allDestinations;
    }

    public void setAllDestinations(Collection<D> allDestinations) {
        this.allDestinations = allDestinations;
    }
}
