package com.jd.journalkeeper.rpc.transport.command;

import com.jd.journalkeeper.rpc.protocol.Protocol;

/**
 * 命令调度器工厂
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/16
 */
public interface CommandDispatcherFactory {

    CommandDispatcher getCommandDispatcher(Protocol protocol);
}