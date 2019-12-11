package io.journalkeeper.utils.retry;

import java.util.Set;

/**
 * @author LiYue
 * Date: 2019/12/11
 */
public class SingleDestinationSelector<D> implements DestinationSelector<D> {
    private final D destination;

    public SingleDestinationSelector(D destination) {
        this.destination = destination;
    }

    @Override
    public D select(Set<D> usedDestinations) {
        return destination;
    }
}
