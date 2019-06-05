package com.jd.journalkeeper.coordinating.test;

import com.jd.journalkeeper.coordinating.network.codec.CoordinatingCodec;
import com.jd.journalkeeper.coordinating.network.command.CompareAndSetRequest;
import com.jd.journalkeeper.coordinating.network.command.CompareAndSetResponse;
import com.jd.journalkeeper.coordinating.network.command.CoordinatingCommand;
import com.jd.journalkeeper.coordinating.network.command.ExistRequest;
import com.jd.journalkeeper.coordinating.network.command.ExistResponse;
import com.jd.journalkeeper.coordinating.network.command.GetClusterRequest;
import com.jd.journalkeeper.coordinating.network.command.GetRequest;
import com.jd.journalkeeper.coordinating.network.command.GetResponse;
import com.jd.journalkeeper.coordinating.network.command.PutRequest;
import com.jd.journalkeeper.coordinating.network.command.PutResponse;
import com.jd.journalkeeper.coordinating.network.command.RemoveRequest;
import com.jd.journalkeeper.coordinating.server.CoordinatingCodes;
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
        Object response = responseCommand.getPayload();
        System.out.println(response);

        responseCommand = transport.sync(new CoordinatingCommand(new PutRequest("test_key".getBytes(), "test_value".getBytes())));
        response = responseCommand.getPayload();
        System.out.println(((PutResponse) response).getModifyTime() + "_" + ((PutResponse) response).getCreateTime());

        responseCommand = transport.sync(new CoordinatingCommand(new GetRequest("test_key".getBytes())));
        response = responseCommand.getPayload();
        System.out.println(new String(((GetResponse) response).getValue()) + "_" + ((GetResponse) response).getModifyTime() + "_" + ((GetResponse) response).getCreateTime());

        responseCommand = transport.sync(new CoordinatingCommand(new ExistRequest("test_key".getBytes())));
        response = responseCommand.getPayload();
        System.out.println(((ExistResponse) response).isExist());

        responseCommand = transport.sync(new CoordinatingCommand(new RemoveRequest("test_key".getBytes())));
        response = responseCommand.getPayload();
        System.out.println(responseCommand.getHeader().getStatus() == CoordinatingCodes.SUCCESS.getType());

        responseCommand = transport.sync(new CoordinatingCommand(new ExistRequest("test_key".getBytes())));
        response = responseCommand.getPayload();
        System.out.println(((ExistResponse) response).isExist());

        responseCommand = transport.sync(new CoordinatingCommand(new CompareAndSetRequest("test_key".getBytes(),
                "test_key".getBytes(), "test_value".getBytes())));
        response = responseCommand.getPayload();
        System.out.println(((CompareAndSetResponse) response).isSuccess());

        responseCommand = transport.sync(new CoordinatingCommand(new CompareAndSetRequest("test_key".getBytes(),
                "test_value_error".getBytes(), "test_value".getBytes())));
        response = responseCommand.getPayload();
        System.out.println(((CompareAndSetResponse) response).isSuccess());

        responseCommand = transport.sync(new CoordinatingCommand(new CompareAndSetRequest("test_key".getBytes(),
                "test_value".getBytes(), "test_value".getBytes())));
        response = responseCommand.getPayload();
        System.out.println(((CompareAndSetResponse) response).isSuccess());
    }
}