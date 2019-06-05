package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;

/**
 * CoordinatingCommandHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public interface CoordinatingCommandHandler extends CommandHandler, Type {
}