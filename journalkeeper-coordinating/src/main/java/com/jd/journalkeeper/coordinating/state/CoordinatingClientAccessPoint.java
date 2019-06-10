package com.jd.journalkeeper.coordinating.state;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.coordinating.state.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.state.domain.StateResponse;
import com.jd.journalkeeper.coordinating.state.domain.StateWriteRequest;
import com.jd.journalkeeper.coordinating.state.serializer.KryoSerializer;
import com.jd.journalkeeper.core.client.Client;
import com.jd.journalkeeper.rpc.RpcAccessPointFactory;
import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import com.jd.journalkeeper.utils.spi.ServiceSupport;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * CoordinatingClientAccessPoint
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/10
 */
public class CoordinatingClientAccessPoint {

    private Properties config;
    private Serializer<StateWriteRequest> entrySerializer;
    private Serializer<StateReadRequest> querySerializer;
    private Serializer<StateResponse> resultSerializer;

    public CoordinatingClientAccessPoint(Properties config) {
        this(config,
                new KryoSerializer(StateWriteRequest.class),
                new KryoSerializer(StateReadRequest.class),
                new KryoSerializer(StateResponse.class));
    }

    public CoordinatingClientAccessPoint(Properties config,
                                         Serializer<StateWriteRequest> entrySerializer,
                                         Serializer<StateReadRequest> querySerializer,
                                         Serializer<StateResponse> resultSerializer) {
        this.config = config;
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
    }

    public CoordinatingClient createClient(List<URI> servers) {
        RpcAccessPointFactory rpcAccessPoint = ServiceSupport.load(RpcAccessPointFactory.class);
        ClientServerRpcAccessPoint clientServerRpcAccessPoint = rpcAccessPoint.createClientServerRpcAccessPoint(servers, config);
        Client<StateWriteRequest, StateReadRequest, StateResponse> client = new Client<>(clientServerRpcAccessPoint, entrySerializer, querySerializer, resultSerializer, config);
        return new CoordinatingClient(servers, config, client);
    }
}