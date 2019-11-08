package io.journalkeeper.core.entry.reserved;

import java.io.Serializable;
import java.net.URI;

import static io.journalkeeper.core.entry.reserved.ReservedEntryType.TYPE_SET_PREFERRED_LEADER;

/**
 * @author LiYue
 * Date: 2019-09-11
 */
public class SetPreferredLeaderEntry extends ReservedEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    private final URI preferredLeader;
    public SetPreferredLeaderEntry(URI preferredLeader) {
        super(TYPE_SET_PREFERRED_LEADER);
        this.preferredLeader = preferredLeader;
    }

    public URI getPreferredLeader() {
        return preferredLeader;
    }
}
