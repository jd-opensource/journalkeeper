package io.journalkeeper.utils.retry;

import java.util.Set;

/**
 * @author LiYue
 * Date: 2019-09-17
 */
public interface DestinationSelector<O> {
    O select(Set<O> usedDestinations);
}
