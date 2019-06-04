package com.jd.journalkeeper.coordinating.test;

import com.jd.journalkeeper.coordinating.network.codec.CoordinatingCodec;
import com.jd.journalkeeper.coordinating.network.command.CoordinatingCommand;
import com.jd.journalkeeper.coordinating.network.command.GetClusterRequest;
import com.jd.journalkeeper.coordinating.network.command.GetClusterResponse;
import com.jd.journalkeeper.rpc.remoting.transport.IpUtil;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.TransportClient;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.config.ClientConfig;
import com.jd.journalkeeper.rpc.remoting.transport.support.DefaultTransportClientFactory;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CoordinatingServerTest {

    @Test
    public void test() {
        DefaultTransportClientFactory transportClientFactory = new DefaultTransportClientFactory(new CoordinatingCodec());
        TransportClient transportClient = transportClientFactory.create(new ClientConfig());
        Transport transport = transportClient.createTransport(new InetSocketAddress(IpUtil.getLocalIp(), 50081));

        Command responseCommand = transport.sync(new CoordinatingCommand(new GetClusterRequest()));
        GetClusterResponse response = (GetClusterResponse) responseCommand.getPayload();
        System.out.println(response);
    }
}