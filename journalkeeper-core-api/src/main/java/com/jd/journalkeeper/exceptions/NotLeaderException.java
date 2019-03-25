package com.jd.journalkeeper.exceptions;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class NotLeaderException extends RuntimeException {
    private final URI leader;


    public NotLeaderException(URI leader) {
        this.leader = leader;
    }

    public URI getLeader() {
        return leader;
    }
}
