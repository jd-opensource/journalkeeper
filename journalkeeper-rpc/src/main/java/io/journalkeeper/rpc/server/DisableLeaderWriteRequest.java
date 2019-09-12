package io.journalkeeper.rpc.server;

/**
 * @author LiYue
 * Date: 2019-09-12
 */
public class DisableLeaderWriteRequest {
    // timeout in ms before leader re-enable write service
    private final long timeoutMs;
    private final int term;

    public DisableLeaderWriteRequest(long timeoutMs, int term) {
        this.timeoutMs = timeoutMs;
        this.term = term;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public int getTerm() {
        return term;
    }
}
