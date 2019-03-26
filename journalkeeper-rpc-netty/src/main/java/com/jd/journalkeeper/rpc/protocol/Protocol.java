package com.jd.journalkeeper.rpc.protocol;

import com.jd.journalkeeper.rpc.transport.codec.CodecFactory;
import com.jd.journalkeeper.rpc.transport.command.handler.CommandHandlerFactory;

/**
 * 协议
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/13
 */
public interface Protocol {

    CodecFactory createCodecFactory();

    CommandHandlerFactory createCommandHandlerFactory();

    String type();
}