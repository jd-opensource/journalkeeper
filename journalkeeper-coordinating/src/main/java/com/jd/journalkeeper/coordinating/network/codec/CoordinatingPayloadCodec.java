package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodec;
import com.jd.journalkeeper.rpc.remoting.transport.command.Payload;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;

/**
 * CoordinatingPayloadCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public interface CoordinatingPayloadCodec<T extends Payload> extends PayloadCodec<CoordinatingHeader, T>, Type {
}