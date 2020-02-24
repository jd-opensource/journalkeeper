package io.journalkeeper.core.server;

import io.journalkeeper.utils.threads.ExceptionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedByInterruptException;

/**
 * @author LiYue
 * Date: 2020/2/24
 */
class DefaultExceptionListener implements ExceptionListener {
    private static final Logger logger = LoggerFactory.getLogger(DefaultExceptionListener.class);
    private final String name;

    public DefaultExceptionListener(String name) {
        this.name = name;
    }

    @Override
    public void onException(Throwable t) {
        try {
            throw t;
        } catch (InterruptedException | ClosedByInterruptException ignored) {
        } catch (Throwable e) {
            if (e.getCause() == null || (!(e.getCause() instanceof ClosedByInterruptException) && !(e.getCause() instanceof InterruptedException))) {
                logger.warn("{} Exception: ", name, e);
            }
        }
    }
}