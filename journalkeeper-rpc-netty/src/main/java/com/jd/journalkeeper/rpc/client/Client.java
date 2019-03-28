package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.transport.Transport;
import com.jd.journalkeeper.rpc.transport.TransportClient;
import com.jd.journalkeeper.rpc.transport.config.ClientConfig;
import com.jd.journalkeeper.rpc.transport.support.DefaultTransportClientFactory;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class Client {
    public static void main(String [] args) {
        DefaultTransportClientFactory defaultTransportClientFactory = new DefaultTransportClientFactory(new TestCodec());
        TransportClient transportClient = defaultTransportClientFactory.create(new ClientConfig());
        Transport transport = transportClient.createTransport("127.0.0.1:9999");

        TestInterface testTpc = new TestInterfaceStub(transport);
        System.out.println("Response from server: " + testTpc.hello("JournalKeeper"));
    }
}
