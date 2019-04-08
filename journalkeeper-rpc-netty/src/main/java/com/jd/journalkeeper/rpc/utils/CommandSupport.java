package com.jd.journalkeeper.rpc.utils;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.RpcException;
import com.jd.journalkeeper.rpc.StatusCode;
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
    public static <T> Command newRequestCommand(int type, T request){
        JournalKeeperHeader header = new JournalKeeperHeader(Direction.REQUEST, type);
        return new Command(header, new GenericPayload<>(request));
    }

    public static Command newRequestCommand(int type){
        JournalKeeperHeader header = new JournalKeeperHeader(Direction.REQUEST, type);
        return new Command(header, new VoidPayload());
    }

    public static <Q, R extends BaseResponse> CompletableFuture<R> sendRequest(Q request, int requestType, Transport transport) {
        CompletableFuture<R> future = new CompletableFuture<>();
        transport.async(
                null == request ? newRequestCommand(requestType): newRequestCommand(requestType, request),
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
        return future.thenApply(response -> {
            if(!response.success() && response.getStatusCode() == StatusCode.EXCEPTION) {
                throw new RpcException(response);
            } else {
                return response;
            }
        });
    }

    public static void sendResponse(BaseResponse response, int responseType,  Command requestCommand, Transport transport) {
        Command responseCommand = newResponseCommand(response, responseType, requestCommand);
        transport.acknowledge(requestCommand, responseCommand);
    }

    public static Command newResponseCommand(BaseResponse response, int responseType, Command requestCommand) {
        Header requestHeader = requestCommand.getHeader();
        JournalKeeperHeader header = new JournalKeeperHeader(Direction.RESPONSE, requestHeader.getRequestId(), responseType);
        header.setStatus(response.getStatusCode().getCode());
        header.setError(response.getError());

        return new Command(header,new GenericPayload<>(response));
    }
}
