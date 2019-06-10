package com.jd.journalkeeper.coordinating.state;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.coordinating.state.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.state.domain.StateResponse;
import com.jd.journalkeeper.coordinating.state.domain.StateWriteRequest;
import com.jd.journalkeeper.coordinating.state.serializer.KryoSerializer;
import com.jd.journalkeeper.coordinating.state.state.CoordinatorStateFactory;
import com.jd.journalkeeper.core.api.RaftServer;
import com.jd.journalkeeper.core.api.StateFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * CoordinatingServerAccessPoint
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/10
 */
public class CoordinatingServerAccessPoint {

    private Properties config;
    private StateFactory<StateWriteRequest, StateReadRequest, StateResponse> stateFactory;
    private Serializer<StateWriteRequest> entrySerializer;
    private Serializer<StateReadRequest> querySerializer;
    private Serializer<StateResponse> resultSerializer;

    public CoordinatingServerAccessPoint(Properties config) {
        this(config,
                new CoordinatorStateFactory(),
                new KryoSerializer(StateWriteRequest.class),
                new KryoSerializer(StateReadRequest.class),
                new KryoSerializer(StateResponse.class));
    }

    public CoordinatingServerAccessPoint(Properties config,
                                         StateFactory<StateWriteRequest, StateReadRequest, StateResponse> stateFactory) {
        this(config,
                stateFactory,
                new KryoSerializer(StateWriteRequest.class),
                new KryoSerializer(StateReadRequest.class),
                new KryoSerializer(StateResponse.class));
    }

    public CoordinatingServerAccessPoint(Properties config,
                                         StateFactory<StateWriteRequest, StateReadRequest, StateResponse> stateFactory,
                                         Serializer<StateWriteRequest> entrySerializer,
                                         Serializer<StateReadRequest> querySerializer,
                                         Serializer<StateResponse> resultSerializer) {
        this.config = config;
        this.stateFactory = stateFactory;
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
    }

    public CoordinatingServer createServer(URI current, List<URI> servers, RaftServer.Roll role) {
        return new CoordinatingServer(current, servers, config, role, stateFactory, entrySerializer, querySerializer, resultSerializer);
    }
}