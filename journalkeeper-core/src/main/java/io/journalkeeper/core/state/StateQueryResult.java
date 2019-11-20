package io.journalkeeper.core.state;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public class StateQueryResult<QR> {
    private final QR result;
    private final long lastApplied;

    public StateQueryResult(QR result, long lastApplied) {
        this.result = result;
        this.lastApplied = lastApplied;
    }

    public QR getResult() {
        return result;
    }

    public long getLastApplied() {
        return lastApplied;
    }
}
