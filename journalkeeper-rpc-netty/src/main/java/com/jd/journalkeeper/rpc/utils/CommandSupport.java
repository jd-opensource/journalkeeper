package com.jd.journalkeeper.rpc.utils;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.payload.VoidPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.CommandCallback;
import com.jd.journalkeeper.rpc.remoting.transport.command.Direction;
import com.jd.journalkeeper.rpc.remoting.transport.command.Header;

import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class CommandSupport {
    public static <T> Command newRequest(int type, T request){
        JournalKeeperHeader header = new JournalKeeperHeader(Direction.REQUEST, type);
        return new Command(header, new GenericPayload<>(request));
    }

    public static Command newRequest(int type){
        JournalKeeperHeader header = new JournalKeeperHeader(Direction.REQUEST, type);
        return new Command(header, new VoidPayload());
    }

    public static <Q, R> CompletableFuture<R> sendRequest(Q request, int requestType, Transport transport) {
        CompletableFuture<R> future = new CompletableFuture<>();
        transport.async(
                null == request ? newRequest(requestType): newRequest(requestType, request),
                new CommandCallback() {
                    @Override
                    public void onSuccess(Command request, Command response) {
                        future.complete(GenericPayload.get(response.getPayload()));
                    }

                    @Override
                    public void onException(Command request, Throwable cause) {
                        future.completeExceptionally(cause);
                    }
                });
        return future;

    }

    public static void sendResponse(Object response, int responseType,  Command requestCommand, Transport transport) {
        Header requestHeader = requestCommand.getHeader();
        JournalKeeperHeader header = new JournalKeeperHeader(Direction.RESPONSE, requestHeader.getRequestId(), responseType);
        Command responseCommand = new Command(header,new GenericPayload<>(response));
        transport.acknowledge(requestCommand, responseCommand);
    }
}
