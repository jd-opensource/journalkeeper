package com.jd.journalkeeper.rpc.transport.command.handler;

import com.jd.journalkeeper.rpc.transport.Transport;
import com.jd.journalkeeper.rpc.transport.command.Command;

/**
 * 异常处理器
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/13
 */
public interface ExceptionHandler {

    void handle(Transport transport, Command command, Throwable throwable);
}