package com.jd.journalkeeper.rpc.remoting.transport.command.provider;

import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

import java.util.concurrent.ExecutorService;

/**
 * 执行线程提供
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/13
 */
public interface ExecutorServiceProvider {

    ExecutorService getExecutorService(Transport transport, Command command);
}