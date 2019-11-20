package io.journalkeeper.core.state;

import io.journalkeeper.core.api.State;

import java.nio.file.Path;

/**
 * 快照
 * @author LiYue
 * Date: 2019/11/20
 */
public class Snapshot<E, ER, Q, QR> {
    private final Path path;
    private final State<E, ER, Q, QR> state;
    private final InternalState reservedState;
    private final long lastIncludedIndex;
    private final int lastIncludedTerm;

    public Snapshot(Path path, State<E, ER, Q, QR> state, InternalState reservedState, long lastIncludedIndex, int lastIncludedTerm) {
        this.path = path;
        this.state = state;
        this.reservedState = reservedState;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
    }

    public State<E, ER, Q, QR> getState() {
        return state;
    }

    public InternalState getReservedState() {
        return reservedState;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public Path getPath() {
        return path;
    }
}
