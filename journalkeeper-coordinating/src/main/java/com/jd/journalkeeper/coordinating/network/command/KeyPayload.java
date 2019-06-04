package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.rpc.remoting.transport.command.Payload;

/**
 * KeyPayload
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public interface KeyPayload extends Payload {

    byte[] getKey();
}