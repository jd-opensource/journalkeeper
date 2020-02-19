package io.journalkeeper.core.serialize;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public interface WrappedStateFactory<E, ER, Q, QR> {
     WrappedState<E, ER, Q, QR> createState();
}
