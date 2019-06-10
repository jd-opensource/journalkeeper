package com.jd.journalkeeper.coordinating.client.handler;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.BooleanResponse;
import com.jd.journalkeeper.coordinating.network.command.PublishWatchRequest;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PublishWatcherRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/10
 */
public class PublishWatcherRequestHandler implements CommandHandler, Type {

    protected static final Logger logger = LoggerFactory.getLogger(PublishWatcherRequestHandler.class);

    @Override
    public Command handle(Transport transport, Command command) {
        PublishWatchRequest publishWatchRequest = (PublishWatchRequest) command.getPayload();

        logger.info("watch type: {}, key: {}, value: {}, modifyTime: {}, createTime: {}",
                publishWatchRequest.getType(), new String(publishWatchRequest.getKey()),
                new String(ObjectUtils.defaultIfNull(publishWatchRequest.getValue(), "null".getBytes())), publishWatchRequest.getModifyTime(), publishWatchRequest.getCreateTime());
        return BooleanResponse.build();
    }

    @Override
    public int type() {
        return CoordinatingCommands.PUBLISH_WATCH_REQUEST.getType();
    }
}