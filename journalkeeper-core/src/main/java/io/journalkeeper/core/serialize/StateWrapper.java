package io.journalkeeper.core.serialize;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateResult;

import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public class StateWrapper<E, ER, Q, QR> implements State, Flushable {
    private final SerializeExtensionPoint serializeExtensionPoint;
    private final WrappedState<E, ER, Q, QR> wrappedState;
    private final Flushable flushable;

    public StateWrapper(WrappedState<E, ER, Q, QR> wrappedState, SerializeExtensionPoint serializeExtensionPoint) {
        this.serializeExtensionPoint = serializeExtensionPoint;
        this.wrappedState = wrappedState;
        if (wrappedState instanceof Flushable) {
            flushable = (Flushable) wrappedState;
        } else {
            flushable = null;
        }
    }

    @Override
    public StateResult execute(byte[] entry, int partition, long index, int batchSize, RaftJournal journal) {
        WrappedStateResult<ER> wrappedStateResult = wrappedState.executeAndNotify(
                serializeExtensionPoint.parse(entry)
        );
        return new StateResult(
                serializeExtensionPoint.serialize(wrappedStateResult.getResult()),
                wrappedStateResult.getEventData());
    }

    @Override
    public byte[] query(byte[] query, RaftJournal journal) {
        return serializeExtensionPoint.serialize(
                wrappedState.query(
                        serializeExtensionPoint.parse(query)
                )
        );
    }

    @Override
    public void recover(Path path, Properties properties) throws IOException {
        wrappedState.recover(path, properties);
    }

    @Override
    public void close() {
        wrappedState.close();
    }

    @Override
    public void flush() throws IOException {
        if (null != flushable) {
            flushable.flush();
        }
    }

}
