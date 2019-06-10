package com.jd.journalkeeper.coordinating.server.watcher;

import com.jd.journalkeeper.coordinating.network.command.CoordinatingCommand;
import com.jd.journalkeeper.coordinating.network.command.PublishWatchRequest;
import com.jd.journalkeeper.coordinating.server.config.CoordinatingWatcherConfig;
import com.jd.journalkeeper.coordinating.server.domain.CoordinatingValue;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.CommandCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * WatcherPublisher
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/10
 */
public class WatcherPublisher {

    protected static final Logger logger = LoggerFactory.getLogger(WatcherPublisher.class);

    private CoordinatingWatcherConfig config;
    private WatcherManager watcherManager;

    public WatcherPublisher(CoordinatingWatcherConfig config, WatcherManager watcherManager) {
        this.config = config;
        this.watcherManager = watcherManager;
    }

    public boolean notifyKeyChanged(byte[] key, CoordinatingValue value) {
        PublishWatchRequest publishWatchRequest = new PublishWatchRequest();
        publishWatchRequest.setType(WatcherNotificationTypes.KEY_CHANGED.getType());
        publishWatchRequest.setKey(key);
        publishWatchRequest.setValue(value.getValue());
        publishWatchRequest.setModifyTime(value.getModifyTime());
        publishWatchRequest.setCreateTime(value.getCreateTime());
        return notifyWatcher(publishWatchRequest);
    }

    public boolean notifyKeyRemoved(byte[] key) {
        PublishWatchRequest publishWatchRequest = new PublishWatchRequest();
        publishWatchRequest.setType(WatcherNotificationTypes.KEY_REMOVED.getType());
        publishWatchRequest.setKey(key);
        return notifyWatcher(publishWatchRequest);
    }

    protected boolean notifyWatcher(PublishWatchRequest publishWatchRequest) {
        Set<Transport> watchers = watcherManager.getWatchers(publishWatchRequest.getKey());
        if (watchers == null || watchers.isEmpty()) {
            return false;
        }

        CountDownLatch latch = new CountDownLatch(watchers.size());
        for (Transport watcher : watchers) {
            watcher.async(new CoordinatingCommand(publishWatchRequest), config.getPublishTimeout(), new CommandCallback() {
                @Override
                public void onSuccess(Command request, Command response) {
                    latch.countDown();
                }

                @Override
                public void onException(Command request, Throwable cause) {

                }
            });
        }

        return true;
    }
}