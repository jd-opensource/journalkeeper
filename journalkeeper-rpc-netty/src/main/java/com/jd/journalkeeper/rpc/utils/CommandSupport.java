/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jd.journalkeeper.rpc.utils;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.payload.VoidPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
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

    // TODO 因为异步里还有其他RPC操作，临时改同步
    public static <Q, R extends BaseResponse> CompletableFuture<R> sendRequest(Q request, int requestType, Transport transport) {
        CompletableFuture<R> future = new CompletableFuture<>();
        try {
            Command response = transport.sync(null == request ? newRequestCommand(requestType): newRequestCommand(requestType, request));
            future.complete(GenericPayload.get(response.getPayload()));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
//        transport.async(
//                null == request ? newRequestCommand(requestType): newRequestCommand(requestType, request),
//                new CommandCallback() {
//                    @Override
//                    public void onSuccess(Command request, Command response) {
//                        future.complete(GenericPayload.get(response.getPayload()));
//                    }
//
//                    @Override
//                    public void onException(Command request, Throwable cause) {
//                        future.completeExceptionally(cause);
//                    }
//                });
        return  future;

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
