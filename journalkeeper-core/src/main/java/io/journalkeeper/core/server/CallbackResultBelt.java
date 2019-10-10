package io.journalkeeper.core.server;

/**
 * @author LiYue
 * Date: 2019-08-14
 */
interface CallbackResultBelt {
    boolean full();
    void put(Callback callback) throws InterruptedException;

    void callbackBefore(long position);

    void callback(long position, byte [] result);
    void failAll();
}
