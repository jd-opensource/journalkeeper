package com.jd.journalkeeper.coordinating.server.network;

import com.jd.journalkeeper.coordinating.network.codec.CoordinatingCodec;
import com.jd.journalkeeper.coordinating.server.CoordinatingContext;
import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfig;
import com.jd.journalkeeper.rpc.remoting.service.Service;
import com.jd.journalkeeper.rpc.remoting.transport.TransportServer;
import com.jd.journalkeeper.rpc.remoting.transport.support.DefaultTransportServerFactory;

/**
 * CoordinatingServer
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public class CoordinatingServer extends Service {

    private CoordinatingConfig config;
    private CoordinatingContext context;
    private TransportServer server;

    public CoordinatingServer(CoordinatingContext context) {
        this.config = context.getConfig();
        this.context = context;
    }

    @Override
    protected void validate() throws Exception {
        this.server = createTransportServer(config);
    }

    @Override
    protected void doStart() throws Exception {
        this.server.start();
    }

    @Override
    protected void doStop() {
        this.server.stop();
    }

    protected TransportServer createTransportServer(CoordinatingConfig config) {
        DefaultTransportServerFactory transportServerFactory = new DefaultTransportServerFactory(new CoordinatingCodec(),
                new CoordinatingCommandHandlerFactory(context), new CoordinatingExceptionHandler(), context.getServerEventBus());
        return transportServerFactory.bind(config.getServer());
    }
}